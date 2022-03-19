package io.github.victorhsr.spring.kafka.streams101.testing

import io.github.victorhsr.spring.kafka.streams101.testing.schema.ElectronicOrder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component


@Component
class AggregationStream(
    @Value("\${kafka.stream.electronic_order.topic}") private val electronicOrderTopic: String,
    @Value("\${kafka.stream.electronic_aggregation.topic}") private val electronicAggregateTopic: String,
    private val avroSerdes: AvroSerdes
) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(AggregationStream::class.java)
    }

    @Autowired
    fun doAggregate(streamsBuilder: StreamsBuilder) {

        val stringSerde = Serdes.String()
        val doubleSerde = Serdes.Double()
        val electronicOrderSerde = this.avroSerdes.serdeOf<ElectronicOrder>()

        streamsBuilder.stream(
            this.electronicOrderTopic, Consumed.with(stringSerde, electronicOrderSerde)
        ).peek { key, value ->
            LOGGER.info("Electronic stream incoming record with key '{}' and value '{}'", key, value)
        }.groupByKey()
            .aggregate(
                { 0.0 },
                this::aggregateElectronicOrderPrice, Materialized.with(stringSerde, doubleSerde)
            )
            .toStream()
            .peek { key, value -> LOGGER.info("Aggregate value for key {} is {}", key, value) }
            .to(this.electronicAggregateTopic, Produced.with(stringSerde, doubleSerde))

    }

    private fun aggregateElectronicOrderPrice(key: String, electronicOrder: ElectronicOrder, acc: Double): Double {
        LOGGER.info("Summing order with key {} and price {} to amount {}", key, electronicOrder.price, acc)
        return acc + electronicOrder.price
    }

}