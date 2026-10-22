package ru.quipy.custom

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.logic.PaymentAccountProperties
import ru.quipy.payments.logic.PaymentFlowLimits
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

data class PaymentTask(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val completion: CompletableFuture<Void> = CompletableFuture(),
)

class PaymentAccountScheduler(
    properties: PaymentAccountProperties,
    private val paymentHandler: (PaymentTask) -> Unit,
    private val failureHandler: (PaymentTask, String) -> Unit,
    queueCapacity: Int = 1_000,
    flowLimits: PaymentFlowLimits = PaymentFlowLimits(properties, properties.averageProcessingTime),
) : AutoCloseable {
    init {
        require(properties.rateLimitPerSec > 0)
        require(properties.parallelRequests > 0)
    }

    private val logger = LoggerFactory.getLogger(javaClass)
    private val queue = LinkedBlockingQueue<PaymentTask>(queueCapacity)
    private val parallelRequests = flowLimits.parallelism
    private val slots = Semaphore(parallelRequests)
    private val workers = Executors.newFixedThreadPool(
        parallelRequests, NamedThreadFactory("payment-${properties.accountName}-http")
    )
    private val dispatcher = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory("payment-${properties.accountName}-dispatcher")
    )
    private var closed = false

    init {
        // Wake up to fill free workers as a batch; rate limiting happens at the network boundary.
        dispatcher.scheduleWithFixedDelay(
            { dispatchAvailable() }, 0, 1, TimeUnit.MILLISECONDS
        )
    }

    @Synchronized
    fun submit(task: PaymentTask) {
        when {
            closed -> fail(task, "Payment scheduler stopped")
            !queue.offer(task) -> fail(task, "Payment queue full")
        }
    }

    @Synchronized
    private fun dispatchAvailable() {
        while (!closed && queue.isNotEmpty() && slots.tryAcquire()) {
            val task = queue.remove()
            try {
                workers.execute {
                    try {
                        if (System.currentTimeMillis() >= task.deadline) {
                            fail(task, "Payment deadline expired")
                        } else {
                            paymentHandler(task)
                        }
                    } catch (e: Exception) {
                        logger.error("Payment {} failed", task.paymentId, e)
                        fail(task, "Payment execution failed")
                    } finally {
                        slots.release()
                        task.completion.complete(null)
                    }
                }
            } catch (e: Exception) {
                slots.release()
                fail(task, "Payment worker unavailable")
            }
        }
    }

    private fun fail(task: PaymentTask, reason: String) {
        try {
            failureHandler(task, reason)
        } catch (e: Exception) {
            logger.error("Could not record failure for payment {}", task.paymentId, e)
        } finally {
            task.completion.complete(null)
        }
    }

    fun queueSize(): Int = queue.size
    fun activePayments(): Int = parallelRequests - slots.availablePermits()

    @Synchronized
    override fun close() {
        if (closed) return
        closed = true
        dispatcher.shutdown()
        workers.shutdown()
        while (true) {
            val task = queue.poll() ?: break
            fail(task, "Payment scheduler stopped")
        }
    }
}
