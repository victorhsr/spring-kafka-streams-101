package io.github.victorhsr.spring.kafka.streams101.timeconcepts

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import java.util.*

class KafkaTestConsumer {

    private val receivedData = mutableMapOf<String, MutableList<Double>>()

    companion object {
        private val LOGGER = LoggerFactory.getLogger(KafkaTestConsumer::class.java)
    }

    @KafkaListener(topics = ["\${kafka.stream.electronic_windowing.topic}"])
    fun consumeOrders(consumerRecord: ConsumerRecord<String, Double>) {

        LOGGER.info("Received message with key = {} and value = {}", consumerRecord.key(), consumerRecord.value())

        if (!receivedData.containsKey(consumerRecord.key())) {
            receivedData[consumerRecord.key()] = mutableListOf(consumerRecord.value())
        } else {
            receivedData[consumerRecord.key()]!!.add(consumerRecord.value())
        }
    }

    fun receivedData(): Map<String, MutableList<Double>> = Collections.unmodifiableMap(this.receivedData)

}