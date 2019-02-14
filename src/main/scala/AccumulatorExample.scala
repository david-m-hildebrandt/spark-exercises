import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object AccumulatorExample {
  // create the SparkConf and the SparkContext
  val sparkConf : SparkConf = new SparkConf().setMaster("local[4]").setAppName("Accumulator Example")
  val sparkContext : SparkContext = SparkContext.getOrCreate(sparkConf)

  // generate a Seq of random integers from 0 to 10
  def createRandomSeq(size: Int) : Seq[Int] = {
    (0 until size).map(i => Random.nextInt(11))
  }

  def main(a : Array[String]) : Unit = {
    val size = 10000000
    val randomRdd = sparkContext.parallelize(createRandomSeq(size))
    val acc = sparkContext.longAccumulator

    randomRdd.foreach(v => acc.add(v))

    println(s"Average is: ${acc.value.toDouble/size.toDouble}")
    println(s"Average is: ${acc.avg}")
  }
}
