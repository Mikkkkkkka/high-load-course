package ru.quipy.custom

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.logic.PaymentAccountProperties
import ru.quipy.payments.logic.PaymentFlowLimits
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
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
    private val workers = ThreadPoolExecutor(
        flowLimits.parallelism, flowLimits.parallelism, 0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(queueCapacity), NamedThreadFactory("payment-${properties.accountName}-http"),
        ThreadPoolExecutor.AbortPolicy(),
    )

    @Synchronized
    fun submit(task: PaymentTask) {
        if (workers.isShutdown) {
            fail(task, "Payment scheduler stopped")
            return
        }
        try {
            workers.execute(PaymentJob(task))
        } catch (e: RejectedExecutionException) {
            fail(task, "Payment queue full")
        }
    }

    private inner class PaymentJob(val task: PaymentTask) : Runnable {
        override fun run() {
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
                task.completion.complete(null)
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

    fun queueSize(): Int = workers.queue.size
    fun activePayments(): Int = workers.activeCount

    @Synchronized
    override fun close() {
        if (workers.isShutdown) return
        workers.shutdown()
        while (true) {
            val job = workers.queue.poll() as PaymentJob? ?: break
            fail(job.task, "Payment scheduler stopped")
        }
    }
}
