package io.github.victorhsr.spring.kafka.streams101.processingapi

import io.github.victorhsr.spring.kafka.streams101.processingapi.schema.ElectronicOrder
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.File
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.collections.set


@SpringBootTest
@Testcontainers
@Import(KafkaTestConsumer::class)
internal class AggregationStreamTest {

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
    private lateinit var producer: KafkaTemplate<String, SpecificRecord>

    @Autowired
    private lateinit var testConsumer: KafkaTestConsumer

    @Value("\${kafka.stream.electronic_order.topic}")
    private lateinit var electronicOrderTopic: String

    @Test
    fun `should sum the price of all electronic-order with the same id`() {

        val electronicOrders = this.initOrders()
        this.publishOrders(electronicOrders)

        val expectedResult = electronicOrders
            .groupBy { order -> order.electronicId }
            .mapValues { entry -> entry.value.map { it.price }.fold(0.0) { acc, orderPrice -> acc + orderPrice } }


        Awaitility.await().atMost(15, TimeUnit.SECONDS).until({ this.testConsumer.receivedData() }, {
            if (it.size != expectedResult.size) return@until false

            it.all { receivedEntry ->
                expectedResult[receivedEntry.key] == receivedEntry.value
            }
        })

    }

    private fun initOrders(): List<ElectronicOrder> {

        var instant = Instant.now()

        val electronicOrderOne = ElectronicOrder.newBuilder()
            .setElectronicId("HDTV-2333")
            .setOrderId("instore-1")
            .setUserId("10261998")
            .setPrice(2000.00)
            .setTime(instant.toEpochMilli()).build()

        instant = instant.plusSeconds(10L)

        val electronicOrderTwo = ElectronicOrder.newBuilder()
            .setElectronicId("HDTV-2333")
            .setOrderId("instore-1")
            .setUserId("1033737373")
            .setPrice(1999.23)
            .setTime(instant.toEpochMilli()).build()

        instant = instant.plusSeconds(10L)

        val electronicOrderThree = ElectronicOrder.newBuilder()
            .setElectronicId("HDTV-2333")
            .setOrderId("instore-1")
            .setUserId("1026333")
            .setPrice(4500.00)
            .setTime(instant.toEpochMilli()).build()

        instant = instant.plusSeconds(12L)

        val electronicOrderFour = ElectronicOrder.newBuilder()
            .setElectronicId("HDTV-2333")
            .setOrderId("instore-1")
            .setUserId("1038884844")
            .setPrice(1333.98)
            .setTime(instant.toEpochMilli()).build()

        instant = instant.plusSeconds(30L)
        val electronicOrderFive = ElectronicOrder.newBuilder()
            .setElectronicId("HDTV-2333")
            .setOrderId("instore-1")
            .setUserId("1038884844")
            .setPrice(1333.98)
            .setTime(instant.toEpochMilli()).build()


        return listOf(
            electronicOrderOne,
            electronicOrderTwo,
            electronicOrderThree,
            electronicOrderFour,
            electronicOrderFive
        )
    }

    private fun publishOrders(orders: List<ElectronicOrder>) {
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

}

class KDockerComposeContainer(file: File) : DockerComposeContainer<KDockerComposeContainer>(file)

class KafkaTestConsumer {

    private val receivedData = mutableMapOf<String, Double>()

    companion object {
        private val LOGGER = LoggerFactory.getLogger(KafkaTestConsumer::class.java)
    }

    @KafkaListener(topics = ["\${kafka.stream.electronic_aggregation.topic}"])
    fun consumeOrders(consumerRecord: ConsumerRecord<String, Double>) {
        LOGGER.info("Received message with key = {} and value = {}", consumerRecord.key(), consumerRecord.value())
        this.receivedData[consumerRecord.key()] = consumerRecord.value()
    }

    fun receivedData(): Map<String, Double> = Collections.unmodifiableMap(this.receivedData)

}
