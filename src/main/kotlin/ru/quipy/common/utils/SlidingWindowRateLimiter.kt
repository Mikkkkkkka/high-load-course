package ru.quipy.common.utils

import java.net.SocketTimeoutException
import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    init {
        require(rate > 0)
        require(!window.isNegative && !window.isZero)
    }

    private val windowNanos = window.toNanos()
    private val starts = ArrayDeque<Long>()
    private val lock = ReentrantLock()
    private val changed = lock.newCondition()

    override fun tick(): Boolean = lock.withLock { tryAcquire(nanoTime()) }

    fun tickBlocking() = awaitPermit(null)

    fun tickBlocking(deadline: Long) = awaitPermit(deadline)

    private fun awaitPermit(deadline: Long?) = lock.withLock {
        while (true) {
            val remainingMillis = deadline?.minus(System.currentTimeMillis())
            if (remainingMillis != null && remainingMillis <= 0) {
                throw SocketTimeoutException("Payment deadline expired")
            }
            val now = nanoTime()
            if (tryAcquire(now)) return
            val waitNanos = windowNanos - (now - starts.first)
            val remainingNanos = remainingMillis?.let { TimeUnit.MILLISECONDS.toNanos(it) } ?: Long.MAX_VALUE
            changed.awaitNanos(minOf(waitNanos, remainingNanos))
        }
    }

    private fun tryAcquire(now: Long): Boolean {
        while (starts.isNotEmpty() && now - starts.first >= windowNanos) starts.removeFirst()
        if (starts.size.toLong() >= rate) return false
        starts.addLast(now)
        return true
    }
}
