package io.github.victorhsr.spring.kafka.streams101.processingapi.topology

import io.github.victorhsr.spring.kafka.streams101.processingapi.schema.ElectronicOrder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.slf4j.LoggerFactory
import java.time.Duration


class OrderAggregatePriceProcessorSupplier(private val storeName: String) :
    ProcessorSupplier<String, ElectronicOrder, String, Double> {

    override fun get(): Processor<String, ElectronicOrder, String, Double> = OrderAggregatePriceProcessor(storeName)

    override fun stores(): MutableSet<StoreBuilder<*>> {
        return mutableSetOf(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(this.storeName),
                Serdes.String(), Serdes.Double()
            )
        )
    }
}

class OrderAggregatePriceProcessor(private val storeName: String) : Processor<String, ElectronicOrder, String, Double> {
    private lateinit var context: ProcessorContext<String, Double>
    private lateinit var store: KeyValueStore<String, Double>

    companion object {
        private val LOGGER = LoggerFactory.getLogger(OrderAggregatePriceProcessor::class.java)
    }

    override fun init(context: ProcessorContext<String, Double>) {
        this.context = context
        store = context.getStateStore(storeName);
        this.context.schedule(Duration.ofSeconds(30), PunctuationType.STREAM_TIME, this::forwardAll);
    }

    private fun forwardAll(punctuationTimestamp: Long) {
        this.store.all().use { storeIterator ->
            while (storeIterator.hasNext()) {
                val keyValue = storeIterator.next()
                val totalPriceRecord = Record(keyValue.key, keyValue.value, punctuationTimestamp)
                this.context.forward(totalPriceRecord)
                LOGGER.info(
                    "Punctuation forwarded a record with key {} and value {}",
                    totalPriceRecord.key(),
                    totalPriceRecord.value()
                )
            }
        }
    }

    override fun process(record: Record<String, ElectronicOrder>) {
        val currentValue = this.store.get(record.key()) ?: 0.0
        val priceToSum = record.value().price

        LOGGER.info("Summing {} to existing value {} for key {}", priceToSum, currentValue, record.key())

        val newValue = currentValue + priceToSum
        this.store.put(record.key(), newValue)
    }

}