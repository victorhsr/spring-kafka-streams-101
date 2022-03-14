package io.github.victorhsr.spring.kafka.streams101

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
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import java.util.*
import java.util.concurrent.TimeUnit

@SpringBootTest
@Import(KafkaTestConsumer::class)
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = ["listeners=PLAINTEXT://localhost:9092", "port=9092"],
    topics = ["\${kafka.stream.order.topic}", "\${kafka.stream.order.stream.separation.output-topic}"]
)
internal class OrderSeparationStreamTest {

    @Autowired
    private lateinit var consumer: KafkaTestConsumer

    @Autowired
    private lateinit var producer: KafkaTemplate<String, String>

    @Value("\${kafka.stream.order.topic}")
    private lateinit var orderTopic: String

    @Value("\${kafka.stream.order.stream.separation.order-number-prefix}")
    private lateinit var orderNumberPrefix: String

    @Test
    fun `should read the messages published in the topic 'order', put the records in a KTable, remove the ones that don't start with the prefix 'orderNumber-' and end with a numeric value greater than 1000, strip the prefix from the remaining messages and then publish the last value into 'last_order_greater_than_1000' topic`() {

        val lastOrderNumber = "8400"
        val messages = mutableListOf(
            "${orderNumberPrefix}999",
            "bogus-1",
            "1001", "${orderNumberPrefix}5000",
            "bogus-2",
            "${orderNumberPrefix}3330",
        )
        messages.add(messages.size, "${orderNumberPrefix}${lastOrderNumber}")

        messages.forEach { this.producer.send(ProducerRecord(this.orderTopic, "order-key", it)) }
        println("messages sent")

        Awaitility.await().atMost(35, TimeUnit.SECONDS)
            .until({ this.consumer.receivedMessages() }, { it.size == 1 && it[0] == lastOrderNumber })
    }

}

class KafkaTestConsumer {

    private val receivedMessages = mutableListOf<String>()

    companion object {
        private val LOGGER = LoggerFactory.getLogger(KafkaTestConsumer::class.java)
    }

    @KafkaListener(topics = ["\${kafka.stream.order.stream.separation.output-topic}"])
    fun receive(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) messageKey: String, @Payload messagePayload: String) {
        LOGGER.info("Received message with key = {} and value = {}", messageKey, messagePayload)
        receivedMessages.add(messagePayload)
    }

    fun receivedMessages(): List<String> = Collections.unmodifiableList(this.receivedMessages)

}