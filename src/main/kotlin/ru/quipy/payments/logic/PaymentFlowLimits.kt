package ru.quipy.payments.logic

import java.time.Duration
import kotlin.math.ceil
import kotlin.math.floor

/** Use the mean for throughput, and the upper bound plus the rate window for the waiting budget. */
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
        throughputPerSecond = minOf(account.rateLimitPerSec.toDouble(), account.parallelRequests / processingSeconds)
        parallelism = ceil(throughputPerSecond * processingSeconds).toInt().coerceIn(1, account.parallelRequests)
        // A send can wait for the one-second rate window before the provider starts work.
        val upperBoundSeconds = processingUpperBound.toNanos() / 1_000_000_000.0
        val waiting = floor(throughputPerSecond * (budgetSeconds - upperBoundSeconds - 1.0).coerceAtLeast(0.0))
        require(waiting <= Int.MAX_VALUE - parallelism) { "Payment buffer is too large" }
        waitingCapacity = waiting.toInt()
        bufferCapacity = parallelism + waitingCapacity
        retryDelayMillis = 1000L
    }
}
