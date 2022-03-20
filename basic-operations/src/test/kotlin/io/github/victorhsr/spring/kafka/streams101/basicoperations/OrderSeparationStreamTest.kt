package io.github.victorhsr.spring.kafka.streams101.basicoperations

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
import org.springframework.kafka.test.context.EmbeddedKafka
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
    fun `should read the messages published in the topic 'order', take the ones that start with the prefix 'orderNumber-' and end with a numeric value greater than 1000, strip it's prefix and then published the value into 'order_greater_than_1000' topic`() {

        val orderNumbersList = listOf("1001", "5000", "3330", "8400")

        val fooOrders = listOf(
            "${orderNumberPrefix}999",
            "bogus-1",
            "bogus-2",
        )

        val messages = mutableListOf<String>()
        messages.addAll(orderNumbersList.map { "$orderNumberPrefix$it" })
        messages.addAll(fooOrders)
        messages.shuffle()

        messages.forEach { this.producer.send(ProducerRecord(orderTopic, null, it)) }

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
            .until({ this.consumer.receivedMessages() }, { it.containsAll(orderNumbersList) })
    }

}

class KafkaTestConsumer {

    private val receivedMessages = mutableListOf<String>()

    companion object {
        private val LOGGER = LoggerFactory.getLogger(KafkaTestConsumer::class.java)
    }

    @KafkaListener(topics = ["\${kafka.stream.order.stream.separation.output-topic}"])
    fun receive(consumerRecord: ConsumerRecord<String, String>) {
        LOGGER.info("Received message= {}", consumerRecord.value())
        receivedMessages.add(consumerRecord.value())
    }

    fun receivedMessages(): List<String> = Collections.unmodifiableList(this.receivedMessages)

}