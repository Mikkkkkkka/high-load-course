package ru.quipy.custom

import org.junit.jupiter.api.Test
import ru.quipy.payments.logic.PaymentAccountProperties
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PaymentCompletionTest {
    private val properties = PaymentAccountProperties("test", "acc-18", 1, 110, 30, Duration.ofSeconds(1), true)

    private fun task(deadline: Long = System.currentTimeMillis() + 3000) =
        PaymentTask(UUID.randomUUID(), 100, System.currentTimeMillis(), deadline)

    @Test
    fun `worker pool bounds concurrency and runs queued payments after completion`() {
        val entered = CountDownLatch(3)
        val release = CountDownLatch(1)
        val active = AtomicInteger()
        val peak = AtomicInteger()
        val payments = List(6) { task(System.currentTimeMillis() + 10000) }
        PaymentAccountScheduler(properties.copy(parallelRequests = 3), {
            val count = active.incrementAndGet()
            peak.updateAndGet { maxOf(it, count) }
            entered.countDown()
            try {
                check(release.await(5, TimeUnit.SECONDS))
            } finally {
                active.decrementAndGet()
            }
        }, { _, reason -> error(reason) }, queueCapacity = 3).use { scheduler ->
            try {
                payments.forEach(scheduler::submit)
                assertTrue(entered.await(5, TimeUnit.SECONDS))
                assertEquals(3, scheduler.activePayments())
                assertEquals(3, scheduler.queueSize())
                assertTrue(payments.none { it.completion.isDone })
            } finally {
                release.countDown()
            }
            payments.forEach { it.completion.get(5, TimeUnit.SECONDS) }
            assertEquals(3, peak.get())
        }
    }

    @Test
    fun `completion waits for handler to finish`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val payment = task()
        PaymentAccountScheduler(properties, {
            entered.countDown()
            check(release.await(5, TimeUnit.SECONDS))
        }, { _, _ -> error("Unexpected failure") }).use { scheduler ->
            try {
                scheduler.submit(payment)
                assertTrue(entered.await(5, TimeUnit.SECONDS))
                assertFalse(payment.completion.isDone)
            } finally {
                release.countDown()
            }
            payment.completion.get(5, TimeUnit.SECONDS)
        }
    }

    @Test
    fun `expired task completes even if recording failure throws`() {
        val failed = CountDownLatch(1)
        val payment = task(0)
        PaymentAccountScheduler(properties, { error("Expired payment must not execute") }, { _, _ ->
            failed.countDown()
            error("Test failure recording error")
        }).use { scheduler ->
            scheduler.submit(payment)
            payment.completion.get(5, TimeUnit.SECONDS)
            assertTrue(failed.await(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun `overflow and shutdown complete tasks without losing reservations`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val active = task()
        val queued = task()
        val overflow = task()
        val stopped = task()
        val scheduler = PaymentAccountScheduler(properties, {
            entered.countDown()
            check(release.await(5, TimeUnit.SECONDS))
        }, { _, _ -> }, queueCapacity = 1)
        try {
            scheduler.submit(active)
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            scheduler.submit(queued)
            scheduler.submit(overflow)
            overflow.completion.get(5, TimeUnit.SECONDS)
            assertFalse(queued.completion.isDone)
            scheduler.close()
            queued.completion.get(5, TimeUnit.SECONDS)
            scheduler.submit(stopped)
            stopped.completion.get(5, TimeUnit.SECONDS)
        } finally {
            release.countDown()
            scheduler.close()
        }
        active.completion.get(5, TimeUnit.SECONDS)
    }
}
