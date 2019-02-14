package trades

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object TradeProcessor {

  val EVENT_DELIMITOR = '|'
  val EVENT_FIELD_DELIMITOR = ','

  /**
    * This trade data would of course come from the file system, HDFS or otherwise
    */
  val tradeData = List(
    "FWD,2905206:09:01,10.56| FWD,2905206:10:53,11.23| FWD,2905206:15:40,23.20",
    "SPOT,2905206:09:04,11.56| FWD,2905206:11:45,11.23| SPOT,2905206:12:30,23.20",
    "FWD,2905206:08:01,10.56| SPOT,2905206:12:30,11.23| FWD,2905206:13:20,23.20| FWD,2905206:14:340,56.00",
    "FWD,2905206:08:01,10.56| SPOT,2905206:12:30,11.23| FWD,2905206:13:20,23.20"
  )

  /**
    * Create the SparkSession, SparkContext and SqlContext objects
    */
  val sparkSession: SparkSession = SparkSession.builder
    .master("local[*]")
    .config("spark.sql.warehouse.dir", System.getProperty("user.home"))
    .enableHiveSupport()
    .getOrCreate
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  /**
    *
    * Simple extraction facility for Strings with delimiters
    * (No error handling is included here for failures)
    *
    * @param tradeAsText
    * @return
    */
  def extract(delimitor: Char)(text: String): List[String] = {
    text.trim.split(delimitor).toList
  }

  /**
    * Partially applied function to extract events
    */
  def extractEvent = extract(EVENT_DELIMITOR) _

  /**
    * Partially applied function to extract event fields
    */
  def extractEventFields = extract(EVENT_FIELD_DELIMITOR) _


  def main(a: Array[String]): Unit = {

    /**
      * Read the records into an RDD of simple Strings
      */
    val tradeRdd = sparkContext.parallelize(tradeData)

    /**
      * Convert the trade data (which would be in a text file on HDFS or the file system)
      * to an 'Event RDD' that includes a trade identifier to group events.
      * The Event RDD is has the form (trade_id, product_id, timestamp, amount)
      */
    val eventRdd = tradeRdd
      .map(extractEvent)
      .zipWithIndex
      .flatMap(
        t => t._1.map(event => (t._2, event.trim)))
      .map(t => (t._1, extractEventFields(t._2)))
      .map(t => (t._1, t._2(0), t._2(1), t._2(2)))

    /**
      * The eventSchema for our DataFrame
      */
    var eventSchema = new StructType()
      .add(StructField("trade_id", LongType, false))
      .add(StructField("product_id", StringType, false))
      .add(StructField("timestamp", StringType, false))
      .add(StructField("amount", FloatType, false))


    /**
      * Create the DataFrame using the RDD and the Schema
      */
    val eventDf = sparkSession.createDataFrame(
      eventRdd.map(t => Row(t._1, t._2, t._3, t._4.toFloat)), eventSchema)

    // Show the contents of the DataFrame
    println
    println("ALL DATA IN THE SPARK EVENT DATA FRAME")
    eventDf.show()

    // Add the view of the DataFrame for use by Spark SQL
    eventDf.createOrReplaceTempView("events")

    // Present the total number of events
    val totalEventNumberDf = sqlContext.sql(
      "SELECT COUNT(*) FROM events")
    println("TOTAL NUMBER OF EVENTS")
    totalEventNumberDf.show

    // Present the distinct product types
    val distinctProductDf = sqlContext.sql(
      "SELECT DISTINCT(product_id) FROM events ORDER BY product_id")
    println("DISTINCT PRODUCT TYPES")
    distinctProductDf.show

    // Present the total amount per product type
    val totalByProductDf = sqlContext.sql(
      "SELECT product_id, sum(amount) FROM events GROUP BY product_id ORDER BY product_id")
    println("TOTAL BY PRODUCT TYPE")
    totalByProductDf.show

    // Present the events in ascending order of time stamp
    val eventsOrderdDf = sqlContext.sql(
      "SELECT timestamp, trade_id, product_id, amount FROM events ORDER BY timestamp, trade_id ASC")
    println("EVENTS IN ASCENDING ORDER BY TIMESTAMP")
    eventsOrderdDf.show

    // Present minimum amount for each product type
    val minimumAmountForEachProductTypeDf = sqlContext.sql(
      "SELECT product_id, MIN(amount) FROM events GROUP BY product_id")
    minimumAmountForEachProductTypeDf.show

    // Present maximum amount for each product type
    val maximumAmountForEachProductTypeDf = sqlContext.sql(
      "SELECT product_id, MAX(amount) FROM events GROUP BY product_id")
    maximumAmountForEachProductTypeDf.show

    /**
      * Save to Hive table
      */
    sqlContext.sql("CREATE TABLE events_hive_table AS SELECT * FROM events")


  }

}
