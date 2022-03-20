package io.github.victorhsr.spring.kafka.streams101.timeconcepts

import io.github.victorhsr.spring.kafka.streams101.timeconcepts.schema.ElectronicOrder
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.File
import java.time.Duration
import java.time.Instant

@Testcontainers
@Import(KafkaTestConsumer::class)
abstract class AbstractWindowingIntegrationTest {

    companion object {
        private const val KAFKA_SERVICE = "kafka_1"
        private const val KAFKA_PORT = 29092

        private const val SCHEMA_REGISTRY_SERVICE = "schemaregistry_1"
        private const val SCHEMA_REGISTRY_PORT = 8081

        @Container
        private val container = KDockerComposeContainer(File("src/test/resources/docker-compose.yml"))
            .withExposedService(
                KAFKA_SERVICE,
                KAFKA_PORT,
                Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(25))
            )
            .withExposedService(
                SCHEMA_REGISTRY_SERVICE,
                SCHEMA_REGISTRY_PORT,
                Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(25))
            )

    }

    @Autowired
    protected lateinit var producer: KafkaTemplate<String, SpecificRecord>

    @Autowired
    protected lateinit var testConsumer: KafkaTestConsumer

    @Value("\${kafka.stream.electronic_order.topic}")
    protected lateinit var electronicOrderTopic: String

    @Value("\${kafka.stream.electronic_windowing.window_size_minutes}")
    protected var windowSizeMinutes: Long? = null

    @Value("\${kafka.stream.electronic_windowing.window_grace_minutes}")
    protected var windowGraceMinutes: Long? = null

    protected fun publishOrders(orders: List<ElectronicOrder>) {
        orders.forEach { electronicOrder: ElectronicOrder ->
            val producerRecord = ProducerRecord<String, SpecificRecord>(
                this.electronicOrderTopic,
                0,
                electronicOrder.time,
                electronicOrder.electronicId,
                electronicOrder
            )
            this.producer.send(producerRecord)
        }
    }

    protected fun calcExpectedResults(orders: List<ElectronicOrder>): Map<String, Double> {
        return orders
            .groupBy { order -> order.electronicId }
            .mapValues { entry -> entry.value.map { it.price }.fold(0.0) { acc, orderPrice -> acc + orderPrice } }
    }

    protected fun isExpectedWindowResult(
        expectedResultForWindow: Map<String, Double>,
        valuesToCheck: Map<String, List<Double>>
    ): Boolean {
        return expectedResultForWindow.all { expectedEntry ->
            if (!valuesToCheck.containsKey(expectedEntry.key)) return false
            val receivedResults = valuesToCheck[expectedEntry.key]!!

            receivedResults.contains(expectedEntry.value)
        }
    }

    protected abstract fun initOrders(): MockOrdersWrapper
}

data class MockOrdersWrapper(
    val orders: List<ElectronicOrder>,
    val windowSize: Duration, // window size + epoch
    val firstMessageAt: Instant,
    val ordersFirstWindow: List<ElectronicOrder>,
    val ordersSecondWindow: List<ElectronicOrder>
)

class KDockerComposeContainer(file: File) : DockerComposeContainer<KDockerComposeContainer>(file)