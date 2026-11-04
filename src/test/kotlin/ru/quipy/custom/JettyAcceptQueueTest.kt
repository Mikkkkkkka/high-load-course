package ru.quipy.custom

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.quipy.config.EventSourcingLibConfiguration
import kotlin.test.assertEquals

class JettyAcceptQueueTest {
    @Test
    fun `accept backlog is configured without requiring HTTP2`() {
        val server = Server()
        try {
            val connector = ServerConnector(server)
            server.addConnector(connector)
            val factory = EventSourcingLibConfiguration().jettyServerCustomizer(1024)
            factory.serverCustomizers.forEach { it.customize(server) }
            assertEquals(1024, connector.acceptQueueSize)
        } finally { server.destroy() }
    }

    @Test
    fun `invalid accept backlog is rejected`() {
        assertThrows<IllegalArgumentException> { EventSourcingLibConfiguration().jettyServerCustomizer(0) }
    }
}
