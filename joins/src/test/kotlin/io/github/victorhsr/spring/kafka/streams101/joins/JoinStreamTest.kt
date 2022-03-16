package io.github.victorhsr.spring.kafka.streams101.joins

import io.github.victorhsr.spring.kafka.streams101.joins.schema.ApplianceOrder
import io.github.victorhsr.spring.kafka.streams101.joins.schema.CombinedOrder
import io.github.victorhsr.spring.kafka.streams101.joins.schema.ElectronicOrder
import io.github.victorhsr.spring.kafka.streams101.joins.schema.User
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
internal class JoinStreamTest {

    companion object {
        private val KAFKA_SERVICE = "kafka_1"
        private val KAFKA_PORT = 29092

        private val SCHEMA_REGISTRY_SERVICE = "schemaregistry_1"
        private val SCHEMA_REGISTRY_PORT = 8081

        @Container
        private val container = KDockerComposeContainer(File("src/test/resources/docker-compose.yml"))
            .withExposedService(
                KAFKA_SERVICE,
                KAFKA_PORT,
                Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(20))
            )
            .withExposedService(
                SCHEMA_REGISTRY_SERVICE,
                SCHEMA_REGISTRY_PORT,
                Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(20))
            )
    }

    @Autowired
    private lateinit var producer: KafkaTemplate<String, SpecificRecord>

    @Autowired
    private lateinit var testConsumer: KafkaTestConsumer

    @Value("\${kafka.stream.user.topic}")
    private lateinit var userTopic: String

    @Value("\${kafka.stream.appliance_order.topic}")
    private lateinit var applianceOrderTopic: String

    @Value("\${kafka.stream.electronic_order.topic}")
    private lateinit var electronicOrderTopic: String

    @Test
    fun `should combine the electronic and appliance orders present in different KStreams and enrich the join result with the user name that is present in a KTable`() {

        val userOneId = "10261998"
        val userTwoId = "10261999"
        val applianceOrderOne = ApplianceOrder.newBuilder()
            .setApplianceId("dishwasher-1333")
            .setOrderId("remodel-1")
            .setUserId(userOneId)
            .setTime(Instant.now().toEpochMilli()).build()

        val applianceOrderTwo = ApplianceOrder.newBuilder()
            .setApplianceId("stove-2333")
            .setOrderId("remodel-2")
            .setUserId(userTwoId)
            .setTime(Instant.now().toEpochMilli()).build()
        val applianceOrders = listOf(applianceOrderOne, applianceOrderTwo)

        val electronicOrderOne = ElectronicOrder.newBuilder()
            .setElectronicId("television-2333")
            .setOrderId("remodel-1")
            .setUserId(userOneId)
            .setTime(Instant.now().toEpochMilli()).build()

        val electronicOrderTwo = ElectronicOrder.newBuilder()
            .setElectronicId("laptop-5333")
            .setOrderId("remodel-2")
            .setUserId(userTwoId)
            .setTime(Instant.now().toEpochMilli()).build()

        val electronicOrders = listOf(electronicOrderOne, electronicOrderTwo)

        val userOne: User =
            User.newBuilder().setUserId("10261998").setAddress("5405 6th Avenue").setName("Elizabeth Jones").build()
        val userTwo: User =
            User.newBuilder().setUserId("10261999").setAddress("407 64th Street").setName("Art Vandelay").build()

        val users = listOf(userOne, userTwo)

        // Users must be created first so the KTable records have earlier timestamps than the KStream records,
        // otherwise the join will ignore them.
        users.forEach { user: User ->
            val producerRecord: ProducerRecord<String, SpecificRecord> =
                ProducerRecord(this.userTopic, user.userId, user)
            this.producer.send(producerRecord)
        }

        applianceOrders.forEach { applianceOrder: ApplianceOrder ->
            val producerRecord =
                ProducerRecord<String, SpecificRecord>(this.applianceOrderTopic, applianceOrder.userId, applianceOrder)
            this.producer.send(producerRecord)
        }

        electronicOrders.forEach { electronicOrder: ElectronicOrder ->
            val producerRecord =
                ProducerRecord<String, SpecificRecord>(
                    this.electronicOrderTopic,
                    electronicOrder.userId,
                    electronicOrder
                )
            this.producer.send(producerRecord)
        }

        Awaitility.await().atMost(15, TimeUnit.SECONDS).until({ this.testConsumer.receivedData() }, {
            if (it.size != 2) return@until false

            val finalOrderUserOne = it[userOneId]
            val finalOrderUserTwo = it[userTwoId]

            if (finalOrderUserOne == null || finalOrderUserTwo == null) return@until false

            finalOrderUserOne.applianceId == applianceOrderOne.applianceId && finalOrderUserOne.applianceOrderId == applianceOrderOne.orderId
                    && finalOrderUserOne.electronicOrderId == electronicOrderOne.orderId
                    && finalOrderUserTwo.applianceId == applianceOrderTwo.applianceId && finalOrderUserTwo.applianceOrderId == applianceOrderTwo.orderId
                    && finalOrderUserTwo.electronicOrderId == electronicOrderTwo.orderId
        })

    }

}

class KDockerComposeContainer(file: File) : DockerComposeContainer<KDockerComposeContainer>(file)

class KafkaTestConsumer {

    private val receivedData = mutableMapOf<String, CombinedOrder>()

    companion object {
        private val LOGGER = LoggerFactory.getLogger(KafkaTestConsumer::class.java)
    }

    @KafkaListener(topics = ["\${kafka.stream.combined_order.topic}"])
    fun consumeOrders(consumerRecord: ConsumerRecord<String, CombinedOrder>) {
        LOGGER.info("Received message with key = {} and value = {}", consumerRecord.key(), consumerRecord.value())
        this.receivedData[consumerRecord.key()] = consumerRecord.value()
    }

    fun receivedData(): Map<String, CombinedOrder> = Collections.unmodifiableMap(this.receivedData)

}
