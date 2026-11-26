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
) : PaymentExternalSystemAdapter, AutoCloseable {

    private data class PaymentAttempt(val task: PaymentTask, val isRetry: Boolean)

    companion object {
        private const val MAX_TEMPORARY_ERROR_RETRIES = 1
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val sendWindow = admission?.sendWindow
        ?: SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), rateWindow)

    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(false)
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .addNetworkInterceptor { chain ->
            val attempt = requireNotNull(chain.request().tag(PaymentAttempt::class.java))
            val task = attempt.task
            sendWindow.tickBlocking(task.deadline)
            if (!attempt.isRetry) {
                admission?.release(task.paymentId)
                queueWait.record(Duration.ofMillis((now() - task.paymentStartedAt).coerceAtLeast(0)))
            }
            chain.proceed(chain.request())
        }
        .build()

    private val scheduler = PaymentAccountScheduler(
        properties = properties,
        paymentHandler = ::performPayment,
        failureHandler = ::recordFailure
    )

    private val queueWait = Timer.builder("payment.queue.wait")
        .tag("account", accountName).register(meterRegistry)

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
        scheduler.submit(
            PaymentTask(
                paymentId = paymentId,
                amount = amount,
                paymentStartedAt = paymentStartedAt,
                deadline = deadline
            )
        )
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
        while (true) {
            val remainingMillis = task.deadline - now()
            if (remainingMillis <= 0) throw SocketTimeoutException("Payment deadline expired")

            val attemptRequest = request.newBuilder()
                .tag(PaymentAttempt::class.java, PaymentAttempt(task, isRetry = retries > 0))
                .build()
            val call = client.newCall(attemptRequest)
            call.timeout().timeout(remainingMillis, TimeUnit.MILLISECONDS)
            val body = call.execute().use { response ->
                try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] Payment parse error, txId=$txId, payment=$paymentId", e)
                    return ExternalSysResponse(txId, paymentId, false, e.message)
                }
            }

            val temporaryFailure = !body.result && body.message == "Temporary error" &&
                    body.transactionId == txId && body.paymentId == paymentId
            if (!temporaryFailure || retries >= MAX_TEMPORARY_ERROR_RETRIES || now() >= task.deadline) {
                return body
            }
            if (task.amount < retryMinAmount) {
                logger.info("[$accountName] Skipping retry for payment $paymentId: amount ${task.amount} below $retryMinAmount")
                return body
            }
            retries++
            logger.info("[$accountName] Retrying payment $paymentId after a temporary error, retry $retries")
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()
