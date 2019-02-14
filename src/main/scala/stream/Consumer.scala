package stream

import java.time.Duration
import java.util.Collections
import java.util.concurrent.Executors

import kafka.utils.Logging
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._

class Consumer extends Logging with Configuration {

  val kafkaConsumer = new KafkaConsumer[String, String](consumerProperties)

  def run() = {
    kafkaConsumer.subscribe(Collections.singletonList(TOPIC))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = kafkaConsumer.poll(Duration.ofMillis(1000))
          for (record <- records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }


}

object Consumer {
  def main(a: Array[String]): Unit = {
    val consumer = new Consumer
    consumer.run()
  }

}


