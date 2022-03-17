package io.github.victorhsr.spring.kafka.streams101.aggregations

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class AggregationsApplication

fun main(args: Array<String>) {
    runApplication<AggregationsApplication>(*args)
}