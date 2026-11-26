package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.*
import java.time.Duration


@Configuration
class PaymentAccountsConfig {
    companion object {
        private val javaClient = HttpClient.newBuilder().build()
        private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())
    }

    @Value("\${payment.hostPort}")
    lateinit var paymentProviderHostPort: String

    @Value("\${payment.service-name}")
    lateinit var serviceName: String

    @Value("\${payment.token}")
    lateinit var token: String

    @Value("#{'\${payment.accounts}'.split(',')}")
    lateinit var allowedAccounts: List<String>

    @Value("\${payment.rate-window-millis:1000}")
    var rateWindowMillis: Long = 1000

    @Value("\${payment.retry-min-amount:0}")
    var retryMinAmount: Int = 0

    @Value("\${payment.acc-7.timeout-millis:0}")
    var case7TimeoutMillis: Long = 0

    @Value("\${payment.acc-7.min-amount:0}")
    var case7MinAmount: Int = 0

    private var adapters: List<PaymentExternalSystemAdapterImpl> = emptyList()

    @PreDestroy
    fun closeAdapters() = adapters.forEach { it.close() }

    @Bean
    fun accountAdapters(
        paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
        meterRegistry: MeterRegistry,
        admission: PaymentAdmission,
    ): List<PaymentExternalSystemAdapter> {
        require(case7TimeoutMillis >= 0) { "Payment provider timeout must be non-negative" }
        require(case7MinAmount >= 0) { "Minimum payment amount must be non-negative" }
        val request = HttpRequest.newBuilder()
            .uri(URI("http://${paymentProviderHostPort}/external/accounts?serviceName=$serviceName&token=$token"))
            .GET()
            .build()

        val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())

        println("\nPayment accounts list:")
        val accounts = mapper.readValue<List<PaymentAccountProperties>>(
            resp.body(),
            mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
        )
            .filter { it.accountName in allowedAccounts }
            .map { it.copy(enabled = true) }
            .onEach(::println)
        if (admission.enabled) {
            require(accounts.size == 1) { "Deadline admission requires one payment account" }
            require(accounts.single().rateLimitPerSec == admission.rate) {
                "Admission rate must match the payment account rate"
            }
        }
        val minAmount = case7MinAmount.takeIf { accounts.singleOrNull()?.accountName == "acc-7" } ?: 0
        adapters = accounts.map {
            PaymentExternalSystemAdapterImpl(
                it,
                paymentService,
                paymentProviderHostPort,
                token,
                meterRegistry,
                Duration.ofMillis(rateWindowMillis),
                admission.takeIf { it.enabled },
                retryMinAmount = retryMinAmount,
                providerTimeoutMillis = if (it.accountName == "acc-7") case7TimeoutMillis else 0,
                minAmount = minAmount,
            )
        }
        return adapters
    }
}
