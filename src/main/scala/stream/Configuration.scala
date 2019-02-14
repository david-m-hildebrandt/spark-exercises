package stream

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

trait Configuration {

  val TOPIC = "data-stream"
  val BROKERS = "localhost:9092"

  val STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"
  val STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer"

  val producerProperties = new Properties()
  producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS)
  producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
  producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER)
  producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER)

  val consumerProperties = new Properties()
  consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS)
  consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1")
  consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  consumerProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
  consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
  consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER)
  consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER)

}
