package io.github.victorhsr.spring.kafka.streams101.joins

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serde
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class AvroSerdes(@Value("\${spring.kafka.properties.schema.registry.url}") private val schemaRegistryUrl: String) {

    fun <T : SpecificRecord> serdeOf(): Serde<T> {

        val serde: Serde<T> = SpecificAvroSerde()

        val serdeProps = mapOf("schema.registry.url" to this.schemaRegistryUrl)
        serde.configure(serdeProps, false)

        return serde
    }

}