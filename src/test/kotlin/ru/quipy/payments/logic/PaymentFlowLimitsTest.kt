package ru.quipy.payments.logic

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import kotlin.test.assertEquals

class PaymentFlowLimitsTest {
    private val account = PaymentAccountProperties("test", "acc-18", 10000, 110, 30, Duration.ofSeconds(1), true)

    @Test
    fun `case 4 derives all limits from account and budget`() {
        val limits = PaymentFlowLimits(account, Duration.ofSeconds(3), Duration.ofSeconds(2))
        assertEquals(110.0, limits.throughputPerSecond)
        assertEquals(110, limits.parallelism)
        assertEquals(0, limits.waitingCapacity)
        assertEquals(110, limits.bufferCapacity)
        assertEquals(1000L, limits.retryDelayMillis)
    }

    @Test
    fun `parallel request limit can be the bottleneck`() {
        val limits = PaymentFlowLimits(account.copy(parallelRequests = 5, averageProcessingTime = Duration.ofSeconds(2)), Duration.ofSeconds(3))
        assertEquals(2.5, limits.throughputPerSecond)
        assertEquals(5, limits.parallelism)
        assertEquals(0, limits.waitingCapacity)
        assertEquals(5, limits.bufferCapacity)
        assertEquals(1000L, limits.retryDelayMillis)
    }

    @Test
    fun `short budget leaves no nominal waiting room`() {
        val limits = PaymentFlowLimits(account, Duration.ofMillis(500))
        assertEquals(0, limits.waitingCapacity)
        assertEquals(limits.parallelism, limits.bufferCapacity)
    }

    @Test
    fun `subsecond processing is calculated without truncation`() {
        val limits = PaymentFlowLimits(account.copy(rateLimitPerSec = 3, averageProcessingTime = Duration.ofMillis(500)), Duration.ofSeconds(1))
        assertEquals(3.0, limits.throughputPerSecond)
        assertEquals(2, limits.parallelism)
        assertEquals(0, limits.waitingCapacity)
        assertEquals(2, limits.bufferCapacity)
        assertEquals(1000L, limits.retryDelayMillis)
    }

    @Test
    fun `longer budget allows waiting after reserving processing and rate window`() {
        val limits = PaymentFlowLimits(account, Duration.ofSeconds(5), Duration.ofSeconds(2))
        assertEquals(220, limits.waitingCapacity)
        assertEquals(330, limits.bufferCapacity)
    }

    @Test
    fun `invalid account or budget is rejected`() {
        assertThrows<IllegalArgumentException> { PaymentFlowLimits(account.copy(rateLimitPerSec = 0), Duration.ofSeconds(3)) }
        assertThrows<IllegalArgumentException> { PaymentFlowLimits(account.copy(parallelRequests = 0), Duration.ofSeconds(3)) }
        assertThrows<IllegalArgumentException> { PaymentFlowLimits(account.copy(averageProcessingTime = Duration.ZERO), Duration.ofSeconds(3)) }
        assertThrows<IllegalArgumentException> { PaymentFlowLimits(account, Duration.ofMillis(-1)) }
        assertThrows<IllegalArgumentException> { PaymentFlowLimits(account, Duration.ofSeconds(3), Duration.ofMillis(500)) }
    }
}
