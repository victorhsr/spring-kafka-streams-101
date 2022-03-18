package io.github.victorhsr.spring.kafka.streams101.processingapi.topology

import io.github.victorhsr.spring.kafka.streams101.processingapi.AvroSerdes
import io.github.victorhsr.spring.kafka.streams101.processingapi.schema.ElectronicOrder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component


@Component
class ProcessingStream(
    @Value("\${kafka.stream.electronic_order.topic}") private val electronicOrderTopic: String,
    @Value("\${kafka.stream.electronic_aggregation.topic}") private val electronicAggregateTopic: String,
    private val avroSerdes: AvroSerdes
) {

    @Autowired
    fun createTopologyWithProcessingApi(streamsBuilder: StreamsBuilder) {

        val stringSerde = Serdes.String()
        val doubleSerde = Serdes.Double()
        val electronicOrderSerde = this.avroSerdes.serdeOf<ElectronicOrder>()

        val stateStoreName = "total-price-store"
        val sourceProcessorName = "source-node"
        val aggregatePriceProcessorName = "aggregate-price"
        val sinkNodeProcessorName = "sink-node"

        val topology = streamsBuilder.build()
        topology.addSource(
            sourceProcessorName,
            stringSerde.deserializer(),
            electronicOrderSerde.deserializer(),
            this.electronicOrderTopic
        )

        topology.addProcessor(
            aggregatePriceProcessorName,
            OrderAggregatePriceProcessorSupplier(stateStoreName),
            sourceProcessorName
        )

        topology.addSink(
            sinkNodeProcessorName,
            this.electronicAggregateTopic,
            stringSerde.serializer(),
            doubleSerde.serializer(),
            aggregatePriceProcessorName
        )
    }

}