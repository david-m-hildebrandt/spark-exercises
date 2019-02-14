package stream

import java.util.Date

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

class Producer extends Configuration {

  val kafkaProducer = new KafkaProducer[String, String](producerProperties)

  def start : Unit = {

    val events = 10
    val t = System.currentTimeMillis()
    val rnd = new Random()

    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new ProducerRecord[String, String](TOPIC, ip, msg)

      //async
      //producer.send(data, (m,e) => {})
      //sync
      kafkaProducer.send(data)
    }

    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
    kafkaProducer.close()

  }

}

object Producer {
  def main(a: Array[String]): Unit = {
    val producer = new Producer()
    producer.start
  }
}
