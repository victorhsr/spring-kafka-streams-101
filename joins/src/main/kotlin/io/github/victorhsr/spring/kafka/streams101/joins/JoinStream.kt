package io.github.victorhsr.spring.kafka.streams101.joins

import io.github.victorhsr.spring.kafka.streams101.joins.schema.ApplianceOrder
import io.github.victorhsr.spring.kafka.streams101.joins.schema.CombinedOrder
import io.github.victorhsr.spring.kafka.streams101.joins.schema.ElectronicOrder
import io.github.victorhsr.spring.kafka.streams101.joins.schema.User
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant


@Component
class JoinStream(
    @Value("\${kafka.stream.user.topic}") private val userTopic: String,
    @Value("\${kafka.stream.appliance_order.topic}") private val applianceOrderTopic: String,
    @Value("\${kafka.stream.electronic_order.topic}") private val electronicOrderTopic: String,
    @Value("\${kafka.stream.combined_order.topic}") private val combinedOrderTopic: String,
    @Value("\${kafka.stream.combined_order.join_windows_seconds}") private val joinWindowsInSeconds: Long,
    private val avroSerdes: AvroSerdes
) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(JoinStream::class.java)
    }

    @Autowired
    fun doJoin(streamsBuilder: StreamsBuilder) {

        val stringSerde = Serdes.String()
        val applianceOrderSerde = this.avroSerdes.serdeOf<ApplianceOrder>()
        val electronicOrderSerde = this.avroSerdes.serdeOf<ElectronicOrder>()
        val userSerde = this.avroSerdes.serdeOf<User>()
        val combinedOrderSerde = this.avroSerdes.serdeOf<CombinedOrder>()

        val applianceOrderKStream = streamsBuilder.stream(
            this.applianceOrderTopic, Consumed.with(stringSerde, applianceOrderSerde)
        ).peek { key, value ->
            LOGGER.info("Appliance stream incoming record with key '{}' and value '{}'", key, value)
        }

        val electronicOrderKStream = streamsBuilder.stream(
            this.electronicOrderTopic, Consumed.with(stringSerde, electronicOrderSerde)
        ).peek { key, value ->
            LOGGER.info("Electronic stream incoming record with key '{}' and value '{}'", key, value)
        }

        val userKTable =
            streamsBuilder.table(
                this.userTopic,
                Consumed.with(stringSerde, userSerde)
            )

        val combinedOrderKStream = applianceOrderKStream.join(
            electronicOrderKStream,
            this::joinOrders,
            JoinWindows.of(Duration.ofSeconds(this.joinWindowsInSeconds)),
            StreamJoined.with(stringSerde, applianceOrderSerde, electronicOrderSerde)
        )
            .peek { key, value -> LOGGER.info("Combined order stream join with key {} and value {}", key, value) }

        combinedOrderKStream.leftJoin(
            userKTable,
            this::fulFillCombinedOrder,
            Joined.with(stringSerde, combinedOrderSerde, userSerde)
        )
            .peek { key, value -> LOGGER.info("Enrichment order stream with key {} and value {}", key, value) }
            .to(this.combinedOrderTopic, Produced.with(stringSerde, combinedOrderSerde))

    }

    private fun joinOrders(applianceOrder: ApplianceOrder, electronicOrder: ElectronicOrder) =
        CombinedOrder.newBuilder()
            .setApplianceOrderId(applianceOrder.orderId)
            .setApplianceId(applianceOrder.applianceId)
            .setElectronicOrderId(electronicOrder.orderId)
            .setTime(Instant.now().toEpochMilli()).build()

    private fun fulFillCombinedOrder(combinedOrder: CombinedOrder, user: User?) =
        user?.let {
            combinedOrder.userName = it.name
            combinedOrder
        }

}