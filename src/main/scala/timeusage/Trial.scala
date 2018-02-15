import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Trial {

  val sparkSession: SparkSession = SparkSession.builder.master("local").getOrCreate
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  //  val sqlContext = new SQLContext(sparkContext)

  import sqlContext.implicits._

  def main(args: Array[String]): Unit = {

    /*
        val schema = StructType(
          StructField("place", IntegerType, false) ::
            StructField("placename", StringType, false) ::
            StructField("amount", IntegerType, false) ::
            Nil)
            */
    var schema = new StructType()
    schema = schema.add(StructField("place", IntegerType, false))
    schema = schema.add(StructField("placename", StringType, false))
    schema = schema.add(StructField("amount", IntegerType, false))

    println("schema.size: " + schema.size)


    //    val simpleDf = sparkContext.parallelize(List((1, "One", 20), (2, "Two", 30))).toDF()
    val simpleRdd = sparkContext.parallelize(List((1, "One", 20), (2, "Two", 30)))
    // name the columns
    // create schema for teh columns

    val simpleDf = sparkSession.createDataFrame(simpleRdd.map(t => Row(t._1, t._2, t._3)), schema)

    println(simpleDf.show())
    println(simpleDf.printSchema())

    val colAddition: Column = $"place" + $"amount"
    //    val colTranslation : Column =

    def amountToSize: UserDefinedFunction = {
      udf((amount: Integer) => {
        if (amount < 25) "small" else "big"
      })
    }

    val colAmountToSize = amountToSize(simpleDf("amount"))

    val colNames = List("place", "amount")
    val columns = List(new Column("place"), new Column("amount"))

    def columnsAddByName(df: DataFrame, colNames: List[String]): Column = colNames match {
      case Nil => new Column("")
      case name :: Nil => df.col(name)
      case name :: tail => df.col(name) + columnsAddByName(df, tail)
    }

    def columnsAdd(df: DataFrame, colNames: List[Column]): Column = colNames match {
      case Nil => new Column("")
      case column :: Nil => column
      case column :: tail => column + columnsAdd(df, tail)
    }

    val colNamesAdded = columnsAddByName(simpleDf, colNames)
    val columnsAdded = columnsAdd(simpleDf, columns)


    //    val colNamesAdded = simpleDf("place") + simpleDf("amount")
    //    val colNamesAdded = colNames.map(name => simpleDf(name)).sum
    //    implicit val colNamesAdded = colNames.fold(0)((v1, v2) => simpleDf(v1) + simpleDf(v2))

    //    val colNamesAdded = colNames.map(name => String.valueOf(simpleDf(name))).sum

    //    val projectedDf = simpleDf.select($"place" + $"amount")
    //    val projectedDf = simpleDf.select(colAddition, $"amount").withColumn("size", amountToSize(simpleDf("amount")))

    val projectedDf = simpleDf.select(colAddition, colAmountToSize, colNamesAdded, columnsAdded)

    println(projectedDf.show())
    println(projectedDf.printSchema())

  }
}




