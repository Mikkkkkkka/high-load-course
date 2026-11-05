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
) : RateLimiter {
    init {
        require(rate > 0)
        require(!window.isNegative && !window.isZero)
    }

    private val windowNanos = window.toNanos()
    private val starts = ArrayDeque<Long>()
    private val lock = ReentrantLock(true)
    private val sendOrder = ReentrantLock(true)
    private val changed = lock.newCondition()

    override fun tick(): Boolean = lock.withLock { tryAcquire(System.nanoTime()) }

    fun estimatedDelayMillis(queuedAhead: Int): Long = lock.withLock {
        require(queuedAhead >= 0)
        val now = System.nanoTime()
        expire(now)
        val available = LongArray(rate.toInt()) { now }
        val free = available.size - starts.size
        starts.forEachIndexed { index, start -> available[free + index] = start + windowNanos }
        val candidateStart = available[queuedAhead % available.size] +
            queuedAhead / available.size * windowNanos
        val delay = (candidateStart - now).coerceAtLeast(0)
        TimeUnit.NANOSECONDS.toMillis(delay + 999_999)
    }

    fun tickBlocking(deadline: Long = Long.MAX_VALUE) {
        val remaining = deadline - System.currentTimeMillis()
        if (remaining <= 0 || !sendOrder.tryLock(remaining, TimeUnit.MILLISECONDS)) {
            throw SocketTimeoutException("Payment deadline expired")
        }
        try {
            lock.withLock {
                while (true) {
                    val remainingMillis = deadline - System.currentTimeMillis()
                    if (remainingMillis <= 0) throw SocketTimeoutException("Payment deadline expired")
                    val now = System.nanoTime()
                    if (tryAcquire(now)) return
                    val waitNanos = windowNanos - (now - starts.first)
                    changed.awaitNanos(minOf(waitNanos, TimeUnit.MILLISECONDS.toNanos(remainingMillis)))
                }
            }
        } finally {
            sendOrder.unlock()
        }
    }

    private fun tryAcquire(now: Long): Boolean {
        expire(now)
        if (starts.size.toLong() >= rate) return false
        starts.addLast(now)
        return true
    }

    private fun expire(now: Long) {
        while (starts.isNotEmpty() && now - starts.first >= windowNanos) starts.removeFirst()
    }
}
