package ru.quipy.payments.logic

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PaymentAdmissionBufferTest {
    @Test
    fun `cold start accepts one payment and grows after completion`() {
        val buffer = PaymentAdmissionBuffer(3)
        assertTrue(buffer.tryAcquire())
        assertFalse(buffer.tryAcquire())
        buffer.release()
        assertEquals(2, buffer.currentLimit())
        assertTrue(buffer.tryAcquire())
        assertTrue(buffer.tryAcquire())
        assertFalse(buffer.tryAcquire())
        buffer.release()
        assertEquals(3, buffer.currentLimit())
        buffer.release()
        assertEquals(3, buffer.currentLimit())
        assertEquals(0, buffer.size())
    }

    @Test
    fun `single place buffer never grows beyond capacity`() {
        val buffer = PaymentAdmissionBuffer(1)
        repeat(10) {
            assertTrue(buffer.tryAcquire())
            assertFalse(buffer.tryAcquire())
            buffer.release()
        }
        assertEquals(1, buffer.currentLimit())
        assertEquals(0, buffer.size())
    }
}
