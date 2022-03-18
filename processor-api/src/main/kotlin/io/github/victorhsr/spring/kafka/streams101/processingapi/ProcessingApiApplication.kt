package io.github.victorhsr.spring.kafka.streams101.processingapi

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ProcessingApiApplication

fun main(args: Array<String>) {
    runApplication<ProcessingApiApplication>(*args)
}