package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Gauge
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.UUID

@Component
class PaymentAdmission(
    @param:Value("\${payment.admission.enabled:false}") val enabled: Boolean,
    @param:Value("\${payment.admission.rate:11}") val rate: Int,
    @Value("\${payment.rate-window-millis:1000}") windowMillis: Long,
    @param:Value("\${payment.admission.processing-reserve-millis:2150}") private val processingReserveMillis: Long,
    meterRegistry: MeterRegistry,
) {
    init {
        require(processingReserveMillis > 0)
    }

    val sendWindow = SlidingWindowRateLimiter(rate.toLong(), Duration.ofMillis(windowMillis))
    private val waiting = LinkedHashSet<UUID>()
    val retryDelayMillis = (1_000L + rate - 1) / rate
    private val accepted = meterRegistry.counter("payment.admission", "outcome", "accepted")
    private val rejected = meterRegistry.counter("payment.admission", "outcome", "rejected")

    init {
        Gauge.builder("payment.admission.waiting", this) {
            synchronized(it) { it.waiting.size.toDouble() }
        }.register(meterRegistry)
    }

    @Synchronized
    fun tryAcquire(paymentId: UUID, deadline: Long): Boolean {
        if (!enabled) return true
        val allowed = deadline - System.currentTimeMillis() >=
            sendWindow.estimatedDelayMillis(waiting.size) + processingReserveMillis
        if (allowed) waiting.add(paymentId)
        if (allowed) accepted.increment() else rejected.increment()
        return allowed
    }

    @Synchronized
    fun release(paymentId: UUID) {
        waiting.remove(paymentId)
    }
}
