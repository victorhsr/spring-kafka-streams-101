package io.github.victorhsr.spring.kafka.streams101.processingapi

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serde
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class AvroSerdes(@Value("\${spring.kafka.properties.schema.registry.url}") private val schemaRegistryUrl: String) {

    fun <T : SpecificRecord> serdeOf(): Serde<T> {

        val serde: Serde<T> = SpecificAvroSerde()

        val serdeProps = mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to this.schemaRegistryUrl)
        serde.configure(serdeProps, false)

        return serde
    }

}