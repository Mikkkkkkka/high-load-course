package ru.quipy.payments.logic

import java.time.Duration
import kotlin.math.ceil
import kotlin.math.floor

/** Limit admission by mean throughput and reserve workers for the slowest expected responses. */
class PaymentFlowLimits(
    account: PaymentAccountProperties,
    processingBudget: Duration,
    processingUpperBound: Duration = account.averageProcessingTime,
) {
    val throughputPerSecond: Double
    val parallelism: Int
    val waitingCapacity: Int
    val bufferCapacity: Int
    val retryDelayMillis: Long

    init {
        require(account.rateLimitPerSec > 0)
        require(account.parallelRequests > 0)
        require(!account.averageProcessingTime.isNegative && !account.averageProcessingTime.isZero)
        require(!processingBudget.isNegative)
        require(processingUpperBound >= account.averageProcessingTime)

        val processingSeconds = account.averageProcessingTime.toNanos() / 1_000_000_000.0
        val budgetSeconds = processingBudget.toNanos() / 1_000_000_000.0
        val upperBoundSeconds = processingUpperBound.toNanos() / 1_000_000_000.0
        throughputPerSecond = minOf(account.rateLimitPerSec.toDouble(), account.parallelRequests / processingSeconds)
        parallelism = ceil(throughputPerSecond * upperBoundSeconds)
            .toInt().coerceIn(1, account.parallelRequests)
        // A send can wait for the one-second rate window before the provider starts work.
        val waiting = floor(throughputPerSecond * (budgetSeconds - upperBoundSeconds - 1.0).coerceAtLeast(0.0))
        require(waiting <= Int.MAX_VALUE - parallelism) { "Payment buffer is too large" }
        waitingCapacity = waiting.toInt()
        bufferCapacity = parallelism + waitingCapacity
        // A full buffer frees roughly one place per completion, not once per rate window.
        retryDelayMillis = ceil(1000.0 / throughputPerSecond).toLong().coerceAtLeast(1L)
    }
}
