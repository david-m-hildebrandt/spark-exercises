import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object ByKeyExamples {

  // create the SparkConf and the SparkContext
  val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Accumulator Example")
  val sparkContext: SparkContext = SparkContext.getOrCreate(sparkConf)

  // generate a Seq of random integers from 0 to 10
  def createRandomCharInt(size: Int): Seq[(Char, Int)] = {
    (0 until size).map(i =>
      ((48 + Random.nextInt(77)).toChar,
        Random.nextInt(11)))
  }

  def main(a: Array[String]): Unit = {

    val kvRdd = sparkContext.parallelize(createRandomCharInt(100))
    // list of numbers will be created per key
    val kvGroupByKey = kvRdd.groupByKey()
    // total the numbers within each group (each key)
    kvGroupByKey.sortByKey(ascending = true, numPartitions = 1).map(kv => (kv._1, kv._2.sum)).take(10).foreach(println)

    println("break")
    // totals the numbers within each key first at the executors, then at the driver
    val kvReduceByKey = kvRdd.reduceByKey(_ + _)
    kvReduceByKey.sortByKey(ascending = true, numPartitions = 1).take(10).foreach(println)
  }

}
