package io.github.victorhsr.spring.kafka.streams101

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
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

    @Autowired
    fun getSeparationStream(streamsBuilder: StreamsBuilder) {

        val kTable: KTable<String, String> = streamsBuilder.table(
            this.orderTopic,
            Materialized.`as`<String, String, KeyValueStore<Bytes, ByteArray>>("ktable-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        )

        kTable
            .filter { _, value -> value.startsWith(this.orderNumberPrefix) }
            .mapValues { value -> value.substringAfter(this.orderNumberPrefix) }
            .filter { _, value -> value.toLong() > this.cutOffValue }
            .toStream()
            .peek { key, value ->
                LOGGER.info(
                    "The last orderNumber for key {} was {}, publishing the result into the {} topic",
                    key,
                    value,
                    this.outputTopic
                )
            }
            .to(this.outputTopic)
    }

}