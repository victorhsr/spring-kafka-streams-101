package io.github.victorhsr.spring.kafka.streams101.basicoperations

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

@Component
class OrderSeparationStream(
    @Value("\${kafka.stream.order.topic}") private val orderTopic: String,
    @Value("\${kafka.stream.order.stream.separation.cut-off-value}") private val cutOffValue: Long,
    @Value("\${kafka.stream.order.stream.separation.order-number-prefix}") private val orderNumberPrefix: String,
    @Value("\${kafka.stream.order.stream.separation.output-topic}") private val outputTopic: String,
) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(OrderSeparationStream.javaClass)
    }

    @Bean
    fun getSeparationStream(streamsBuilder: StreamsBuilder): KStream<String?, String> {
        val stream = streamsBuilder.stream<String?, String>(orderTopic)

        stream
            .peek { key, value -> LOGGER.info("Processing order with key {} and value {}", key, value) }
            .filter { _, value -> value.startsWith(orderNumberPrefix) }
            .mapValues { value -> value.substringAfter(orderNumberPrefix) }
            .filter { _, value -> value.toLong() > cutOffValue }
            .peek { key, value ->
                LOGGER.info(
                    "The order with key {} and value {} have passed by the filter, publishing the record into the {} topic",
                    key,
                    value,
                    outputTopic
                )
            }
            .to(outputTopic)

        return stream
    }

}