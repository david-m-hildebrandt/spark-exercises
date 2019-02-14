package stream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StreamProcessor extends Configuration  {

  import org.apache.spark._
  import org.apache.spark.streaming._

  val conf = new SparkConf().setAppName("streamProcessor").setMaster("local[*]")
  val streamingContext = new StreamingContext(conf, Seconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> BROKERS,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "group-2",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array(TOPIC)
  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

//  stream.map(record => (record.key, record.value))

  def main(a : Array[String]) : Unit = {
    stream.map(record => {
      println(s"record.key: $record.key record.value: $record.value")
      ()
    })
  }

}



