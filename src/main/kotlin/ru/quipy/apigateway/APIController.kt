package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentAdmission
import java.util.*

@RestController
class APIController(
    private val orderRepository: OrderRepository,
    private val orderPayer: OrderPayer,
    private val paymentAdmission: PaymentAdmission,
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): PaymentSubmissionDto {
        val order = orderRepository.findById(orderId)
            ?: throw IllegalArgumentException("No such order $orderId")
        val paymentId = UUID.randomUUID()
        if (!paymentAdmission.tryAcquire(paymentId, deadline)) throw PaymentAdmissionRejected()
        try {
            orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
            return PaymentSubmissionDto(createdAt, paymentId)
        } catch (e: Exception) {
            paymentAdmission.release(paymentId)
            throw e
        }
    }

    class PaymentAdmissionRejected : RuntimeException()

    @ExceptionHandler(PaymentAdmissionRejected::class)
    fun admissionRejected(): ResponseEntity<Void> = ResponseEntity.status(429)
        // The course tester expects an absolute Unix timestamp in milliseconds.
        .header("Retry-After", (System.currentTimeMillis() + paymentAdmission.retryDelayMillis).toString())
        .build()

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
