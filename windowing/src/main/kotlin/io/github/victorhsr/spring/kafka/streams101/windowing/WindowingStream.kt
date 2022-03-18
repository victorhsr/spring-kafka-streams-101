package io.github.victorhsr.spring.kafka.streams101.windowing

import io.github.victorhsr.spring.kafka.streams101.windowing.schema.ElectronicOrder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration


@Component
class WindowingStream(
    @Value("\${kafka.stream.electronic_order.topic}") private val electronicOrderTopic: String,
    @Value("\${kafka.stream.electronic_windowing.topic}") private val electronicAggregateTopic: String,
    @Value("\${kafka.stream.electronic_windowing.window_size_minutes}") private val windowSizeMinutes: Long,
    @Value("\${kafka.stream.electronic_windowing.window_grace_minutes}") private val windowGraceMinutes: Long,
    private val avroSerdes: AvroSerdes
) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(WindowingStream::class.java)
    }

    @Autowired
    fun doWindow(streamsBuilder: StreamsBuilder) {

        val stringSerde = Serdes.String()
        val doubleSerde = Serdes.Double()
        val electronicOrderSerde = this.avroSerdes.serdeOf<ElectronicOrder>()

        streamsBuilder.stream(
            this.electronicOrderTopic, Consumed.with(stringSerde, electronicOrderSerde)
        ).peek { key, value ->
            LOGGER.info("Electronic stream incoming record with key '{}' and value '{}'", key, value)
        }.groupByKey()
            .windowedBy(
                TimeWindows.ofSizeAndGrace(
                    Duration.ofMinutes(this.windowSizeMinutes),
                    Duration.ofMinutes(this.windowGraceMinutes)
                )
            )
            .aggregate(
                { 0.0 },
                this::aggregateElectronicOrderPrice, Materialized.with(stringSerde, doubleSerde)
            )
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .peek { windowedKey, value ->
                LOGGER.info(
                    "Window starts in {} close in {} for Key {} and value {}",
                    windowedKey.window().start(),
                    windowedKey.window().end(),
                    windowedKey.key(),
                    value
                )
            }
            .map { windowedKey, value -> KeyValue.pair(windowedKey.key(), value) }
            .peek { key, value -> LOGGER.info("Aggregate value for key {} is {}", key, value) }
            .to(this.electronicAggregateTopic, Produced.with(stringSerde, doubleSerde))

    }

    private fun aggregateElectronicOrderPrice(key: String, electronicOrder: ElectronicOrder, acc: Double): Double {
        LOGGER.info("Summing order with key {} and price {} to amount {}", key, electronicOrder.price, acc)
        return acc + electronicOrder.price
    }

}