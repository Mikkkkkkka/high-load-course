package ru.quipy.payments.logic

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito
import org.springframework.test.util.ReflectionTestUtils
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import ru.quipy.apigateway.APIController
import ru.quipy.core.EventSourcingService
import ru.quipy.core.BasicAggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.database.EventStore
import ru.quipy.mapper.EventMapper
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentCreatedEvent
import java.util.UUID
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class PaymentBackPressureTest {
    private val registry = SimpleMeterRegistry()
    private val pending = LinkedBlockingQueue<CompletableFuture<Void>>()
    private val submitted = LinkedBlockingQueue<UUID>()

    private val aggregates = BasicAggregateRegistry().apply {
        register(PaymentAggregate::class, PaymentAggregateState::class) {
            registerStateTransition(PaymentCreatedEvent::class, PaymentAggregateState::paymentCreatedApply)
        }
    }
    private val mapper = Mockito.mock(EventMapper::class.java) { invocation ->
        if (invocation.method.name == "eventToString") "{}" else Mockito.RETURNS_DEFAULTS.answer(invocation)
    }
    private val properties = Mockito.mock(EventSourcingProperties::class.java) { invocation ->
        when (invocation.method.name) {
            "getDbSchema" -> "public"
            "getSnapshotFrequency" -> 100
            else -> Mockito.RETURNS_DEFAULTS.answer(invocation)
        }
    }
    private val events = EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>(
        PaymentAggregate::class, aggregates, mapper, properties, Mockito.mock(EventStore::class.java),
    )

    private val service = object : PaymentService {
        override fun submitPaymentRequest(
            paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long,
        ): CompletableFuture<Void> {
            val completion = CompletableFuture<Void>()
            pending.add(completion)
            submitted.add(paymentId)
            return completion
        }
    }
    private val limits = PaymentFlowLimits(
        PaymentAccountProperties("test", "acc-18", 10000, 110, 30, Duration.ofSeconds(1), true),
        Duration.ofSeconds(3),
        Duration.ofSeconds(2),
    )
    private val payer = OrderPayer(registry, events, service, limits, backpressureEnabled = true)

    @AfterEach
    fun close() {
        payer.close()
        pending.forEach { it.complete(null) }
        registry.close()
    }

    private fun submit(onAccepted: () -> Unit = {}) {
        payer.processPayment(UUID.randomUUID(), 100, UUID.randomUUID(), System.currentTimeMillis() + 3000, onAccepted)
    }

    private fun fillBuffer() {
        repeat(limits.bufferCapacity - 1) {
            submit()
            assertNotNull(submitted.poll(5, TimeUnit.SECONDS))
            pending.remove().complete(null)
            awaitBufferSize(0.0)
        }
        repeat(limits.bufferCapacity) { submit() }
        repeat(limits.bufferCapacity) {
            assertNotNull(submitted.poll(5, TimeUnit.SECONDS), "Payment was not submitted")
        }
    }

    @Test
    fun `buffer stays full until external processing completes`() {
        fillBuffer()
        val accepted = AtomicBoolean(false)
        assertThrows<PaymentBufferFullException> { submit { accepted.set(true) } }
        assertFalse(accepted.get())
        assertEquals(limits.bufferCapacity.toDouble(), registry.get("payment.buffer.size").gauge().value())

        pending.remove().complete(null)
        // Completion can race with callback registration in the submission worker.
        awaitBufferSize((limits.bufferCapacity - 1).toDouble())
        submit()
        assertNotNull(submitted.poll(5, TimeUnit.SECONDS))
        assertEquals(limits.bufferCapacity.toDouble(), registry.get("payment.buffer.size").gauge().value())
        assertEquals(1.0, registry.get("payment.admission.rejected").counter().count())
    }

    @Test
    fun `overloaded HTTP request gets 429 without changing order`() {
        fillBuffer()
        val repository = OrderRepository()
        val order = APIController.Order(UUID.randomUUID(), UUID.randomUUID(), 0, APIController.OrderStatus.COLLECTING, 100)
        repository.save(order)
        val controller = APIController()
        ReflectionTestUtils.setField(controller, "orderRepository", repository)
        ReflectionTestUtils.setField(controller, "orderPayer", payer)
        val mvc = MockMvcBuilders.standaloneSetup(controller).build()
        val before = System.currentTimeMillis()

        val response = mvc.perform(
            post("/orders/${order.id}/payment").param("deadline", (before + 3000).toString())
        ).andReturn().response

        assertEquals(429, response.status)
        val retryAt = assertNotNull(response.getHeader("Retry-After")).toLong()
        assertTrue(retryAt >= before + limits.retryDelayMillis)
        assertTrue(retryAt <= System.currentTimeMillis() + limits.retryDelayMillis)
        assertEquals(order, repository.findById(order.id))
    }

    @Test
    fun `submission error releases reserved place`() {
        submit { throw IllegalStateException("Test submission failure") }
        awaitBufferSize(0.0)
    }

    @Test
    fun `exceptional external completion releases reserved place`() {
        submit()
        assertNotNull(submitted.poll(5, TimeUnit.SECONDS))
        pending.remove().completeExceptionally(IllegalStateException("Test external failure"))
        awaitBufferSize(0.0)
    }

    @Test
    fun `stopped executor rejects without accepting or leaking a place`() {
        payer.close()
        val accepted = AtomicBoolean(false)
        assertThrows<PaymentBufferFullException> { submit { accepted.set(true) } }
        assertFalse(accepted.get())
        assertEquals(0.0, registry.get("payment.buffer.size").gauge().value())
    }

    @Test
    fun `disabled back pressure accepts more than the admission limit`() {
        val disabledRegistry = SimpleMeterRegistry()
        val disabled = OrderPayer(disabledRegistry, events, service, limits, backpressureEnabled = false)
        try {
            repeat(limits.bufferCapacity + 100) {
                disabled.processPayment(UUID.randomUUID(), 100, UUID.randomUUID(), System.currentTimeMillis() + 3000) {}
            }
            repeat(limits.bufferCapacity + 100) { assertNotNull(submitted.poll(5, TimeUnit.SECONDS)) }
            assertEquals(limits.bufferCapacity + 100, pending.size)
            assertEquals(0.0, disabledRegistry.get("payment.admission.rejected").counter().count())
            pending.forEach { it.complete(null) }
            assertEquals(0.0, disabledRegistry.get("payment.buffer.size").gauge().value())
        } finally {
            disabled.close()
            disabledRegistry.close()
        }
    }

    @Test
    fun `disabled back pressure does not turn shutdown into 429`() {
        val disabledRegistry = SimpleMeterRegistry()
        try {
            val disabled = OrderPayer(disabledRegistry, events, service, limits, backpressureEnabled = false)
            disabled.close()
            assertThrows<RejectedExecutionException> {
                disabled.processPayment(UUID.randomUUID(), 100, UUID.randomUUID(), System.currentTimeMillis() + 3000) {}
            }
            assertEquals(0.0, disabledRegistry.get("payment.admission.rejected").counter().count())
        } finally {
            disabledRegistry.close()
        }
    }

    private fun awaitBufferSize(expected: Double) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (registry.get("payment.buffer.size").gauge().value() != expected && System.nanoTime() < deadline) {
            Thread.sleep(5)
        }
        assertEquals(expected, registry.get("payment.buffer.size").gauge().value())
    }
}
