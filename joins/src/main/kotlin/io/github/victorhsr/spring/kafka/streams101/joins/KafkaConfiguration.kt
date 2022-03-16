package io.github.victorhsr.spring.kafka.streams101.joins

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration


@EnableKafka
@Configuration
@EnableKafkaStreams
class KafkaConfiguration {

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamConfig(
        @Value(value = "\${spring.kafka.bootstrap-servers}") bootstrapServers: String,
        @Value("\${spring.kafka.streams.application-id}") applicationId: String,
        @Value("\${spring.kafka.properties.schema.registry.url}") schemaRegistryUrl: String,
        avroSerdes: AvroSerdes
    ): KafkaStreamsConfiguration {
        val props = mapOf<String, Any>(
            StreamsConfig.APPLICATION_ID_CONFIG to applicationId,
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl
        )

        return KafkaStreamsConfiguration(props)
    }

}