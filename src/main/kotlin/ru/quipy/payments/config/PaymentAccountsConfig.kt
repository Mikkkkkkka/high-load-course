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

    @Value("\${payment.processing-time-millis:3000}")
    var processingTimeMillis: Long = 3000

    @Value("\${payment.processing-upper-bound-millis:2000}")
    var processingUpperBoundMillis: Long = 2000

    private var adapters: List<PaymentExternalSystemAdapterImpl> = emptyList()

    @PreDestroy
    fun closeAdapters() = adapters.forEach { it.close() }

    @Bean
    fun paymentFlowLimits(accounts: List<PaymentExternalSystemAdapter>): PaymentFlowLimits {
        require(accounts.isNotEmpty()) { "No payment accounts configured" }
        // Each payment is sent to every configured account, so use the smallest buffer.
        return accounts.minBy { it.flowLimits.bufferCapacity }.flowLimits
    }

    @Bean
    fun accountAdapters(
        paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
        meterRegistry: MeterRegistry
    ): List<PaymentExternalSystemAdapter> {
        val request = HttpRequest.newBuilder()
            .uri(URI("http://${paymentProviderHostPort}/external/accounts?serviceName=$serviceName&token=$token"))
            .GET()
            .build()

        val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())

        println("\nPayment accounts list:")
        adapters = mapper.readValue<List<PaymentAccountProperties>>(
            resp.body(),
            mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
        )
            .filter { it.accountName in allowedAccounts }
            .map { it.copy(enabled = true) }
            .onEach(::println)
            .map {
                PaymentExternalSystemAdapterImpl(
                    it,
                    PaymentFlowLimits(it, Duration.ofMillis(processingTimeMillis),
                        maxOf(it.averageProcessingTime, Duration.ofMillis(processingUpperBoundMillis))),
                    paymentService,
                    paymentProviderHostPort,
                    token,
                    meterRegistry
                )
            }
        return adapters
    }
}
