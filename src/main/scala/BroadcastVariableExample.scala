import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object BroadcastVariableExample {
  // create the SparkConf and the SparkContext
  val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Broadcast Variable Example")
  val sparkContext: SparkContext = SparkContext.getOrCreate(sparkConf)

  val MAX_SIZE = 11

  def generateIntMap(max: Int): Map[Int, Int] = {
    (0 to max).map(i => i -> Random.nextInt(max)).toMap
  }

  val intMap: Map[Int, Int] = generateIntMap(MAX_SIZE)

  // generate a Seq of random integers from 0 to 10
  def createRandomSeq(size: Int): Seq[Int] = {
    (0 until size).map(i => Random.nextInt(MAX_SIZE))
  }

  def main(a: Array[String]): Unit = {
    val size = 10000000
    val randomRdd = sparkContext.parallelize(createRandomSeq(size))
    val broadcastIntMap = sparkContext.broadcast(intMap)

    val outcome = randomRdd
      .map(i => {
        val m = broadcastIntMap.value.getOrElse(i, 0)
        (i, i - m)
      })
      .filter(_._1 > 2)
      .map(t => {
        val m = broadcastIntMap.value.getOrElse(t._1, 0)
        (t._1, t._2 * m)
      })
      .filter(_._2 < 0).reduceByKey(_ + _)

    println("outcome")
    outcome.foreach(println)
    println(intMap)
  }
}
