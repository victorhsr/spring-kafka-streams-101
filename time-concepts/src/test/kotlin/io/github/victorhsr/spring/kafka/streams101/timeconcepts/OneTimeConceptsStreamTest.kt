package io.github.victorhsr.spring.kafka.streams101.timeconcepts

import io.github.victorhsr.spring.kafka.streams101.timeconcepts.schema.ElectronicOrder
import org.awaitility.Awaitility
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit


@SpringBootTest
@Testcontainers
@Import(KafkaTestConsumer::class)
internal class OneTimeConceptsStreamTest : AbstractWindowingIntegrationTest() {

    @Test
    fun `should sum the price of all electronic-order with the same id in a window, only the messages from the first window will arrive because the Supressed operator will not be able to detect that the second window has ended`() {

        val mockOrdersWrapper = this.initOrders()
        this.publishOrders(mockOrdersWrapper.orders)

        val firstWindowExpectedResults = this.calcExpectedResults(mockOrdersWrapper.ordersFirstWindow)

        Awaitility.await().atMost(12, TimeUnit.SECONDS).until({ this.testConsumer.receivedData() }, {

            if (it.keys.isEmpty()) return@until false

            this.isExpectedWindowResult(firstWindowExpectedResults, it)
        })
    }

    override fun initOrders(): MockOrdersWrapper {

        val referenceDate = LocalDateTime.of(2022, 3, 18, 0, 0, 0, 0)
        referenceDate.atZone(ZoneId.of("Europe/Paris")).toInstant()

        val firstMessageInstant = referenceDate.atZone(ZoneId.of("Europe/Paris")).toInstant()
        val hdtvKey = "HDTV-2333"
        val wideTvKey = "SUPER-WIDE-TV-2333"

        val firstMessageEpochMilli = firstMessageInstant.toEpochMilli()
        val electronicOrderOne = ElectronicOrder.newBuilder()
            .setElectronicId(hdtvKey)
            .setOrderId("instore-1")
            .setUserId("10261998")
            .setPrice(2000.00)
            .setTime(firstMessageEpochMilli).build()

        val electronicOrderTwo = ElectronicOrder.newBuilder()
            .setElectronicId(hdtvKey)
            .setOrderId("instore-1")
            .setUserId("1033737373")
            .setPrice(1999.23)
            .setTime(firstMessageInstant.plus(15, ChronoUnit.MINUTES).toEpochMilli()).build()

        val electronicOrderThree = ElectronicOrder.newBuilder()
            .setElectronicId(hdtvKey)
            .setOrderId("instore-1")
            .setUserId("1026333")
            .setPrice(4500.00)
            .setTime(firstMessageInstant.plus(30, ChronoUnit.MINUTES).toEpochMilli()).build()

        val electronicOrderFour = ElectronicOrder.newBuilder()
            .setElectronicId(hdtvKey)
            .setOrderId("instore-1")
            .setUserId("1038884844")
            .setPrice(1333.98)
            .setTime(firstMessageInstant.plus(45, ChronoUnit.MINUTES).toEpochMilli()).build()

        val electronicOrderFive = ElectronicOrder.newBuilder()
            .setElectronicId(hdtvKey)
            .setOrderId("instore-1")
            .setUserId("1038884844")
            .setPrice(1333.98)
            .setTime(firstMessageInstant.plus(63, ChronoUnit.MINUTES).toEpochMilli()).build()


        val electronicOrderSix = ElectronicOrder.newBuilder()
            .setElectronicId(wideTvKey)
            .setOrderId("instore-1")
            .setUserId("1038884844")
            .setPrice(5333.98)
            .setTime(firstMessageInstant.plus(63, ChronoUnit.MINUTES).toEpochMilli()).build()

        val electronicOrderSeven = ElectronicOrder.newBuilder()
            .setElectronicId(wideTvKey)
            .setOrderId("instore-1")
            .setUserId("1038884844")
            .setPrice(4333.98)
            .setTime(firstMessageInstant.plus(108, ChronoUnit.MINUTES).toEpochMilli()).build()

        val electronicOrders = listOf(
            electronicOrderOne,
            electronicOrderTwo,
            electronicOrderThree,
            electronicOrderFour,
            electronicOrderFive,
            electronicOrderSix,
            electronicOrderSeven
        )

        val maxWindowEpochMilli = firstMessageEpochMilli + (this.windowSizeMinutes!! * 60L * 1000)

        val ordersFirstWindow = electronicOrders.filter { order ->
            order.time <= maxWindowEpochMilli
        }

        return MockOrdersWrapper(
            orders = electronicOrders,
            windowSize = Duration.ofMinutes(this.windowSizeMinutes!!),
            firstMessageAt = firstMessageInstant,
            ordersFirstWindow = ordersFirstWindow,
            ordersSecondWindow = listOf()
        )
    }

}


