package ru.quipy.payments.logic

/** Bound outstanding payments and grow the initial limit as the processing path warms up. */
class PaymentAdmissionBuffer(private val capacity: Int) {
    init { require(capacity > 0) }

    private var pending = 0
    private var limit = 1

    @Synchronized
    fun tryAcquire(): Boolean {
        if (pending >= limit) return false
        pending++
        return true
    }

    @Synchronized
    fun release() {
        check(pending > 0)
        pending--
        if (limit < capacity) limit++
    }

    @Synchronized
    fun size(): Int = pending

    @Synchronized
    fun currentLimit(): Int = limit
}
