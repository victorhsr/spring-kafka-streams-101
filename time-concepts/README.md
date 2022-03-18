This project was based on the 14th video of the Kafka Streams 101 course by Confluent, link: 
https://developer.confluent.io/learn-kafka/kafka-streams/hands-on-time-concepts/

The key difference between this project and the 'windowing' one, is that before the timestamp of the record was defined by the 
producer and now, this value is extracted from the record itself by our custom timestamp extractor
