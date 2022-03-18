package io.github.victorhsr.spring.kafka.streams101.windowing

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class WindowingApplication

fun main(args: Array<String>) {
    runApplication<WindowingApplication>(*args)
}