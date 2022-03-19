package io.github.victorhsr.spring.kafka.streams101.testing

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.github.victorhsr.spring.kafka.streams101.testing.schema.ElectronicOrder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test


internal class AggregationStreamTest {

    private val stringSerde = Serdes.String()
    private val doubleSerde = Serdes.Double()
    private val electronicSerde = AVRO_SERDES.serdeOf<ElectronicOrder>()

    companion object {
        private const val SCHEMA_REGISTRY_URL = "mock://aggregation-test"
        private val AVRO_SERDES = AvroSerdes(SCHEMA_REGISTRY_URL)

        private const val ELECTRONIC_ORDER_TOPIC: String = "electronic_order"
        private const val ELECTRONIC_TOTAL_PRICE_TOPIC: String = "electronic_aggregation_result"

        private val STREAM_BUILDER = StreamsBuilder()

        @BeforeAll
        @JvmStatic
        fun initAggregationStream() {
            AggregationStream(ELECTRONIC_ORDER_TOPIC, ELECTRONIC_TOTAL_PRICE_TOPIC, AVRO_SERDES).doAggregate(
                STREAM_BUILDER
            )
        }
    }

    @Test
    fun `should sum the price of all electronic-order with the same id`() {

        val electronicOrders = this.initOrders()
        val expectedResult = this.calcExpectedResults(electronicOrders)

        this.createTopologyTestDriver().use { testDriver ->
            val inputTopic = this.createInputTopic(testDriver)
            val outputTopic = this.createOutputTopic(testDriver)

            this.publishOrders(electronicOrders, inputTopic)
            val topicResult = outputTopic.readKeyValuesToMap()

            Assertions.assertEquals(expectedResult, topicResult)
        }

    }

    private fun createOutputTopic(testDriver: TopologyTestDriver): TestOutputTopic<String, Double> {
        return testDriver.createOutputTopic(
            ELECTRONIC_TOTAL_PRICE_TOPIC,
            this.stringSerde.deserializer(),
            this.doubleSerde.deserializer()
        )
    }

    private fun createInputTopic(testDriver: TopologyTestDriver): TestInputTopic<String, ElectronicOrder> {
        return testDriver.createInputTopic(
            ELECTRONIC_ORDER_TOPIC,
            this.stringSerde.serializer(),
            this.electronicSerde.serializer()
        )
    }

    private fun createTopologyTestDriver(): TopologyTestDriver {
        val props = mapOf(
            StreamsConfig.APPLICATION_ID_CONFIG to "test-aggregation",
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to SCHEMA_REGISTRY_URL
        ).toProperties()

        return TopologyTestDriver(STREAM_BUILDER.build(), props)
    }

    private fun calcExpectedResults(electronicOrders: List<ElectronicOrder>): Map<String, Double> {
        return electronicOrders.groupBy { order -> order.electronicId }
            .mapValues { entry -> entry.value.map { it.price }.fold(0.0) { acc, orderPrice -> acc + orderPrice } }
    }

    private fun initOrders(): List<ElectronicOrder> = listOf(
        ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("1").setUserId("vandeley").setTime(5L)
            .setPrice(5.0).build(),
        ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("2").setUserId("penny-packer").setTime(5L)
            .setPrice(15.0).build(),
        ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("3").setUserId("romanov").setTime(5L)
            .setPrice(25.0).build(),
    )

    private fun publishOrders(orders: List<ElectronicOrder>, inputTopic: TestInputTopic<String, ElectronicOrder>) {
        orders.forEach { inputTopic.pipeInput(it.electronicId, it) }
    }

}
