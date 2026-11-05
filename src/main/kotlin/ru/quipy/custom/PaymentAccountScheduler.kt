package ru.quipy.custom

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.logic.PaymentAccountProperties
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

data class PaymentTask(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
)

class PaymentAccountScheduler(
    properties: PaymentAccountProperties,
    private val paymentHandler: (PaymentTask) -> Unit,
    private val failureHandler: (PaymentTask, String) -> Unit,
    queueCapacity: Int = 1_000,
) : AutoCloseable {
    init {
        require(properties.rateLimitPerSec > 0)
        require(properties.parallelRequests > 0)
    }

    private val logger = LoggerFactory.getLogger(javaClass)
    private val queue = LinkedBlockingQueue<PaymentTask>(queueCapacity)
    private val active = AtomicInteger()
    private val workers = Executors.newFixedThreadPool(
        properties.parallelRequests, NamedThreadFactory("payment-${properties.accountName}-http")
    )
    @Volatile private var closed = false

    init {
        repeat(properties.parallelRequests) { workers.execute(::processQueue) }
    }

    @Synchronized
    fun submit(task: PaymentTask) {
        when {
            closed -> fail(task, "Payment scheduler stopped")
            !queue.offer(task) -> fail(task, "Payment queue full")
        }
    }

    private fun processQueue() {
        while (!closed) {
            val task = try {
                queue.poll(1, TimeUnit.SECONDS) ?: continue
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }
            active.incrementAndGet()
            try {
                if (System.currentTimeMillis() >= task.deadline) {
                    fail(task, "Payment deadline expired")
                } else {
                    // The network interceptor enforces the rate at the actual HTTP send.
                    paymentHandler(task)
                }
            } catch (e: Exception) {
                logger.error("Payment {} failed", task.paymentId, e)
                fail(task, "Payment execution failed")
            } finally {
                active.decrementAndGet()
            }
        }
    }

    private fun fail(task: PaymentTask, reason: String) {
        try {
            failureHandler(task, reason)
        } catch (e: Exception) {
            logger.error("Could not record failure for payment {}", task.paymentId, e)
        }
    }

    fun queueSize(): Int = queue.size
    fun activePayments(): Int = active.get()

    @Synchronized
    override fun close() {
        if (closed) return
        closed = true
        workers.shutdown()
        while (true) {
            val task = queue.poll() ?: break
            fail(task, "Payment scheduler stopped")
        }
    }
}
