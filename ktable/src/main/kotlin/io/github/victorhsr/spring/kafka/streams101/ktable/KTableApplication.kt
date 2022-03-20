package io.github.victorhsr.spring.kafka.streams101.ktable

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KTableApplication

fun main(args: Array<String>) {
    runApplication<KTableApplication>(*args)
}