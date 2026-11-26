package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.custom.PaymentAccountScheduler
import ru.quipy.custom.PaymentTask
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
    rateWindow: Duration = Duration.ofMillis(1000),
    private val admission: PaymentAdmission? = null,
    private val retryMinAmount: Int = 0,
    private val providerTimeoutMillis: Long = 0,
    private val minAmount: Int = 0,
) : PaymentExternalSystemAdapter, AutoCloseable {

    private enum class RetryReason { TEMPORARY_ERROR, PROVIDER_TIMEOUT }

    private data class PaymentAttempt(val task: PaymentTask, val retryReason: RetryReason?)

    companion object {
        private const val MAX_TEMPORARY_ERROR_RETRIES = 1
        private const val MAX_PROVIDER_TIMEOUT_RETRIES = 2
        private const val RESPONSE_RESERVE_MILLIS = 200
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val sendWindow = admission?.sendWindow
        ?: SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), rateWindow)

    private val retryCounters = RetryReason.entries.associateWith { reason ->
        meterRegistry.counter(
            "payment.http.retries", "service", serviceName, "account", accountName,
            "reason", reason.name.lowercase(),
        )
    }

    private val responseLatency = latencyTimer("response")
    private val timeoutLatency = latencyTimer("provider_timeout")
    private val transportErrorLatency = latencyTimer("transport_error")

    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(false)
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .addNetworkInterceptor { chain ->
            val attempt = requireNotNull(chain.request().tag(PaymentAttempt::class.java))
            val task = attempt.task
            sendWindow.tickBlocking(task.deadline)
            if (attempt.retryReason == null) {
                admission?.release(task.paymentId)
                queueWait.record(Duration.ofMillis((now() - task.paymentStartedAt).coerceAtLeast(0)))
            }
            val request = withProviderTimeout(chain.request(), task.deadline)
            attempt.retryReason?.let { retryCounters.getValue(it).increment() }
            val startedAt = System.nanoTime()
            try {
                val response = chain.proceed(request)
                val timer = if (response.code == HTTP_CLIENT_TIMEOUT) timeoutLatency else responseLatency
                timer.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS)
                response
            } catch (e: IOException) {
                transportErrorLatency.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS)
                throw e
            }
        }
        .build()

    private val scheduler = PaymentAccountScheduler(
        properties = properties,
        paymentHandler = ::performPayment,
        failureHandler = ::recordFailure
    )

    private val queueWait = Timer.builder("payment.queue.wait")
        .tag("account", accountName).register(meterRegistry)

    private fun latencyTimer(outcome: String) = Timer.builder("payment.http.response")
        .description("Time from sending a payment attempt to receiving response headers")
        .tags("service", serviceName, "account", accountName, "outcome", outcome)
        .publishPercentiles(0.5, 0.9, 0.99)
        .register(meterRegistry)

    private fun withProviderTimeout(request: Request, deadline: Long): Request {
        if (providerTimeoutMillis == 0L) return request
        val timeoutMillis = minOf(providerTimeoutMillis, deadline - now() - RESPONSE_RESERVE_MILLIS)
        if (timeoutMillis <= 0) throw SocketTimeoutException("Payment deadline expired")
        val url = request.url.newBuilder()
            .addQueryParameter("timeout", Duration.ofMillis(timeoutMillis).toString())
            .build()
        return request.newBuilder().url(url).build()
    }

    init {
        Gauge.builder("payment.queue.size", scheduler) { it.queueSize().toDouble() }
            .tag("account", accountName).register(meterRegistry)
        Gauge.builder("payment.active", scheduler) { it.activePayments().toDouble() }
            .tag("account", accountName).register(meterRegistry)
    }

    private fun recordFailure(task: PaymentTask, reason: String) {
        admission?.release(task.paymentId)
        meterRegistry.counter("payment.rejected", "account", accountName, "reason", reason).increment()
        val transactionId = UUID.randomUUID()
        paymentESService.update(task.paymentId) {
            it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - task.paymentStartedAt))
        }
        paymentESService.update(task.paymentId) {
            it.logProcessing(false, now(), transactionId, reason)
        }
    }

    override fun close() = scheduler.close()

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        val task = PaymentTask(
            paymentId = paymentId,
            amount = amount,
            paymentStartedAt = paymentStartedAt,
            deadline = deadline
        )
        if (minAmount > 0 && amount < minAmount) {
            recordFailure(task, "Payment amount below minimum")
            return
        }
        scheduler.submit(task)
    }


    private fun performPayment(task: PaymentTask) {
        val paymentId = task.paymentId
        val amount = task.amount
        val paymentStartedAt = task.paymentStartedAt

        logger.warn(
            "[$accountName] Submitting payment request for payment $paymentId"
        )

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(
                success = true,
                transactionId,
                now(),
                Duration.ofMillis(now() - paymentStartedAt)
            )
        }

        logger.info(
            "[$accountName] Prepared: $paymentId , txId: $transactionId"
        )

        try {
            val request = Request.Builder().run {
                url(
                    "http://$paymentProviderHostPort" +
                            "/external/process" +
                            "?serviceName=$serviceName" +
                            "&token=$token" +
                            "&accountName=$accountName" +
                            "&transactionId=$transactionId" +
                            "&paymentId=$paymentId" +
                            "&amount=$amount"
                )
                post(emptyBody)
            }.build()

            val remainingMillis = task.deadline - now()
            if (remainingMillis <= 0) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Payment deadline expired")
                }
                return
            }
            val body = sendPayment(request, task, transactionId)
            logger.warn(
                "[$accountName] Payment processed for " +
                        "txId: $transactionId, " +
                        "payment: $paymentId, " +
                        "succeeded: ${body.result}, " +
                        "message: ${body.message}"
            )
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error(
                        "[$accountName] Payment timeout for " +
                                "txId: $transactionId, payment: $paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            false,
                            now(),
                            transactionId,
                            reason = "Request timeout."
                        )
                    }
                }

                else -> {
                    logger.error(
                        "[$accountName] Payment failed for " +
                                "txId: $transactionId, payment: $paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            false,
                            now(),
                            transactionId,
                            reason = e.message
                        )
                    }
                }
            }
        } finally {
            admission?.release(paymentId)
        }
    }

    private fun sendPayment(request: Request, task: PaymentTask, transactionId: UUID): ExternalSysResponse {
        val txId = transactionId.toString()
        val paymentId = task.paymentId.toString()
        var retries = 0
        var retryReason: RetryReason? = null
        while (true) {
            val remainingMillis = task.deadline - now()
            if (remainingMillis <= 0) throw SocketTimeoutException("Payment deadline expired")

            val attemptRequest = request.newBuilder()
                .tag(PaymentAttempt::class.java, PaymentAttempt(task, retryReason))
                .build()
            val call = client.newCall(attemptRequest)
            call.timeout().timeout(remainingMillis, TimeUnit.MILLISECONDS)
            val (status, body) = call.execute().use { response ->
                try {
                    response.code to mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] Payment parse error, txId=$txId, payment=$paymentId", e)
                    return ExternalSysResponse(txId, paymentId, false, e.message)
                }
            }

            if (body.result || body.transactionId != txId || body.paymentId != paymentId || now() >= task.deadline) {
                return body
            }
            retryReason = when {
                providerTimeoutMillis > 0 && status == HTTP_CLIENT_TIMEOUT && body.message == "Timeout" &&
                    retries < MAX_PROVIDER_TIMEOUT_RETRIES -> RetryReason.PROVIDER_TIMEOUT
                body.message == "Temporary error" && retries < MAX_TEMPORARY_ERROR_RETRIES &&
                    task.amount >= retryMinAmount -> RetryReason.TEMPORARY_ERROR
                else -> return body
            }
            retries++
            logger.info("[$accountName] Retrying payment $paymentId after $retryReason, retry $retries")
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()
