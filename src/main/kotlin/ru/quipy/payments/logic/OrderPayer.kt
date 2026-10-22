package ru.quipy.payments.logic

import jakarta.annotation.PreDestroy
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    meterRegistry: MeterRegistry,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val flowLimits: PaymentFlowLimits,
    @param:Value("\${payment.backpressure.enabled:false}")
    private val backpressureEnabled: Boolean = false,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    // Keep a place reserved until the external payment finishes, including both queues.
    private val bufferSlots = PaymentAdmissionBuffer(flowLimits.bufferCapacity)
    private val rejected = Counter.builder("payment.admission.rejected").register(meterRegistry)

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(if (backpressureEnabled) flowLimits.bufferCapacity else 8_000),
        NamedThreadFactory("payment-submission-executor"),
        if (backpressureEnabled) ThreadPoolExecutor.AbortPolicy() else CallerBlockingRejectedExecutionHandler()
    )

    init {
        Gauge.builder("payment.buffer.size", bufferSlots) { it.size().toDouble() }.register(meterRegistry)
        Gauge.builder("payment.buffer.limit", bufferSlots) { it.currentLimit().toDouble() }.register(meterRegistry)
        Gauge.builder("payment.buffer.capacity", flowLimits) { it.bufferCapacity.toDouble() }.register(meterRegistry)
        logger.info("Payment flow: throughput={}, parallelism={}, waiting={}, buffer={}, retryDelayMs={}, backpressure={}",
            flowLimits.throughputPerSecond, flowLimits.parallelism, flowLimits.waitingCapacity,
            flowLimits.bufferCapacity, flowLimits.retryDelayMillis, backpressureEnabled)
        Gauge.builder(
            "payment_executor_queue_size",
            paymentExecutor
        ) { executor ->
            executor.queue.size.toDouble()
        }
            .register(meterRegistry)

        Gauge.builder(
            "payment_executor_active_threads",
            paymentExecutor
        ) { executor ->
            executor.activeCount.toDouble()
        }
            .register(meterRegistry)
    }

    @PreDestroy
    fun close() = paymentExecutor.shutdown()

    fun processPayment(
        orderId: UUID,
        amount: Int,
        paymentId: UUID,
        deadline: Long,
        onAccepted: () -> Unit,
    ): Long {
        if (backpressureEnabled && !bufferSlots.tryAcquire()) {
            rejected.increment()
            throw PaymentBufferFullException(flowLimits.retryDelayMillis)
        }
        val createdAt = System.currentTimeMillis()
        try {
            paymentExecutor.execute {
                try {
                    onAccepted()
                    val createdEvent = paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                        .whenComplete { _, _ ->
                            if (backpressureEnabled) bufferSlots.release()
                        }
                } catch (e: Exception) {
                    if (backpressureEnabled) bufferSlots.release()
                    logger.error("Could not submit payment {}", paymentId, e)
                }
            }
        } catch (e: RejectedExecutionException) {
            if (!backpressureEnabled) throw e
            bufferSlots.release()
            rejected.increment()
            throw PaymentBufferFullException(flowLimits.retryDelayMillis)
        }
        return createdAt
    }
}

class PaymentBufferFullException(val retryDelayMillis: Long) : RuntimeException("Payment buffer full")
