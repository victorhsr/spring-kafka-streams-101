# Spring Kafka Streams 101

This is a non-official repository with proper implementation of the examples used by Confluent in their course [Kafka Streams 101](https://developer.confluent.io/learn-kafka/kafka-streams/get-started/). The examples are made in Kotlin and uses the **Spring Framework**.


## Motivation

Browsing through Confluent's repositories I found the [original repository](https://github.com/confluentinc/learn-kafka-courses/tree/main/kafka-streams) of the course, and looking into it I tought it was a good idea to make that implementation in Kotlin using the Spring Framework, adding runnable test cases for all of them, making use of JUnit, TestContainers, Spring's Embebed Kafka and of course, Kafka's TopologyTestDriver.

---

**Most of the mock entities in the test cases were taken from the original project in order to make it simpler for who is seeing this repo while take the classes.**