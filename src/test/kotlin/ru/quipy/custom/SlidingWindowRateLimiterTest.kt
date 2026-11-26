package ru.quipy.custom

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SlidingWindowRateLimiterTest {
    @Test
    fun `quota is restored by age rather than fixed window boundaries`() {
        var now = 0L
        val limiter = SlidingWindowRateLimiter(2, Duration.ofSeconds(1)) { now }
        assertTrue(limiter.tick())
        now = TimeUnit.MILLISECONDS.toNanos(500)
        assertTrue(limiter.tick())
        now = TimeUnit.MILLISECONDS.toNanos(999)
        assertFalse(limiter.tick())
        now = TimeUnit.SECONDS.toNanos(1)
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
        now = TimeUnit.MILLISECONDS.toNanos(1500)
        assertTrue(limiter.tick())
    }

    @Test
    fun `concurrent callers cannot exceed the window quota`() {
        val limiter = SlidingWindowRateLimiter(110, Duration.ofSeconds(1)) { 0L }
        val executor = Executors.newFixedThreadPool(16)
        val done = CountDownLatch(300)
        val accepted = AtomicInteger()
        try {
            repeat(300) {
                executor.execute {
                    try { if (limiter.tick()) accepted.incrementAndGet() } finally { done.countDown() }
                }
            }
            assertTrue(done.await(5, TimeUnit.SECONDS))
            assertEquals(110, accepted.get())
        } finally { executor.shutdownNow() }
    }

    @Test
    fun `blocking call expires without sending beyond quota`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofSeconds(1))
        assertTrue(limiter.tick())
        assertThrows<SocketTimeoutException> { limiter.tickBlocking(System.currentTimeMillis() + 40) }
        assertFalse(limiter.tick())
    }

    @Test
    fun `blocking call resumes when the old start leaves the window`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofMillis(50))
        assertTrue(limiter.tick())
        limiter.tickBlocking(System.currentTimeMillis() + 1000)
        assertFalse(limiter.tick())
    }
}
