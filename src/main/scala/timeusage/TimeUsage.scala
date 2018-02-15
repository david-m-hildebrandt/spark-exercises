package timeusage

import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions.{round, udf}
import org.apache.spark.sql.types._

/** Main class */
object TimeUsage {

  import org.apache.spark.sql.SparkSession

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
  }

  def timeUsageByLifePeriod(): Unit = {
//    val (columns, initDf) = read("/timeusage/atussum.csv")
    //val (columns, initDf) = read("/timeusage/atussum.10.csv")
    val (columns, initDf) = read("/timeusage/atussum.10000.csv")
    // val (columns, initDf) = read("/timeusage/atussum.10.40.wide.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)

    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

    //    println("rows start")
    //    summaryDf.take(5).foreach(println(_))
    //    summaryDf.sample(false, .01).show(100)
    //    println("rows stop")
    val finalDf = timeUsageGrouped(summaryDf)
    println("timeUsageGrouped - start")
    finalDf.show()
    println("timeUsageGrouped - stop")
    val extraDf = timeUsageGroupedSql(summaryDf)
    println("timeUsageGroupedSql - start")
    extraDf.show()
    println("timeUsageGroupedSql - stop")

    val dst = timeUsageSummaryTyped(summaryDf)
    //    println("DataSet[TimeUsageRow] - start")
    dst.show
    //    println("DataSet[TimeUsageRow] - stop")

    //    println("DataSet[TimeUsageRow] Summary - start")
    val ds = timeUsageGroupedTyped(dst)

    //    println("DataSet[TimeUsageRow] Summary - stop")
    println("timeUsageGroupedTyped - start")
    ds.show
    println("timeUsageGroupedTyped - stop")
  }

  /** @return The read DataFrame along with its column names. */
  def read(resource: String): (List[String], DataFrame) = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    //    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //    println("schema: " + schema)
    //    schema.printTreeString()
    //    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema)

    //    println("======================================================================")
    //    println("read:" + resource)
    //    println("headerColumns: " + headerColumns)
    //    println("dataFrame.take(1): " + dataFrame.take(1))

    // this line below is empty
    //    println("dataFrame.printSchema(): " + dataFrame. printSchema())


    //    println("read:" + resource)
    //    println("======================================================================")

    (headerColumns, dataFrame)
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  /** @return The schema of the DataFrame, assuming that the first given column has type String and all the others
    *         have type Double. None of the fields are nullable.
    * @param columnNames Column names of the DataFrame
    */
  def dfSchema(columnNames: List[String]): StructType = {
    var structType = new StructType()
    structType = structType.add(StructField(columnNames.head, StringType, false))
    //    println("structType.size: " + structType.size)

    columnNames.tail.foreach(name => structType = structType.add(StructField(name, DoubleType, false)))
    //    columnNames.tail.foreach(name =>structType = structType.add(StructField(name, StringType, false)))
    //    columnNames.tail.foreach(name =>structType = structType.add(StructField(name, IntegerType, false)))
    //    println("columnNames: " + columnNames)

    //    println("================================ structType.printTreeString()")
    //    structType.printTreeString()
    //    println("structType.size: " + structType.size)
    //    println("================================ structType.printTreeString()")
    structType
  }


  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */


  def row(line: List[String]): Row = {
    val revisedLine: List[Any] = line.head +: line.tail.map(_.toDouble)
    val r = Row.fromSeq(revisedLine)
    r
  }

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    *      The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    *      “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
    *
    *      This method groups related columns together:
    * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
    *      “t1801” and “t1803”.
    * 2. working activities. These are the columns starting with “t05” and “t1805”.
    * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
    *      “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {

    var primaryNeedsColumns: List[Column] = List()
    var workingActivitiesColumns: List[Column] = List()
    var othersActivitiesColumns: List[Column] = List()

    def isInGroup(columnName: String, prefixes: List[String]): Boolean = prefixes match {
      case Nil => false
      case prefix :: tail => if (columnName.startsWith(prefix)) true
      else isInGroup(columnName, tail)
    }

    val primaryPrefixes = List("t01", "t03", "t11", "t1801", "t1803")
    val workingPrefixes = List("t05", "t1805")
    val otherPrefixes = List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")

    columnNames.foreach(columnName => {
      val column = new Column(columnName)
      if (isInGroup(columnName, primaryPrefixes)) primaryNeedsColumns = column :: primaryNeedsColumns
      else if (isInGroup(columnName, workingPrefixes)) workingActivitiesColumns = column :: workingActivitiesColumns
      else if (isInGroup(columnName, otherPrefixes)) othersActivitiesColumns = column :: othersActivitiesColumns
    })

    //    println("primaryNeedsColumns: " + primaryNeedsColumns)
    //    println("workingActivitiesColumns: " + workingActivitiesColumns)
    //    println("othersActivitiesColumns: " + othersActivitiesColumns)

    (primaryNeedsColumns, workingActivitiesColumns, othersActivitiesColumns)
  }

  val WORKING = "working"
  val SEX = "sex"
  val AGE = "age"
  val NEEDS = "primaryNeeds"
  val WORK = "work"
  val OTHER = "other"
  val NEEDS_AVG = "avg(primaryNeeds)"
  val WORK_AVG = "avg(work)"
  val OTHER_AVG = "avg(other)"


  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The “teage” column is also
    *         projected to three values: "young", "active", "elder".
    * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
    * @param workColumns         List of columns containing time spent working
    * @param otherColumns        List of columns containing time spent doing other activities
    * @param df                  DataFrame whose schema matches the given column lists
    *
    *                            This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    *                            a single column.
    *
    *                            The resulting DataFrame should have the following columns:
    * - working: value computed from the “telfs” column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the “tesex” column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the “teage” column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    *                            Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
    *
    *                            Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
    */
  def timeUsageSummary(
                        primaryNeedsColumns: List[Column],
                        workColumns: List[Column],
                        otherColumns: List[Column],
                        df: DataFrame
                      ): DataFrame = {

    //    println("timeUsageSummary")

    //    df.printSchema()

    //    println(df.head().schema.printTreeString())

    //    println("timeUsageSummary")

    /** working status
      * - working: value computed from the “telfs” column of the given DataFrame:
      *   - "working" if 1 <= telfs < 3
      *   - "not working" otherwise
      */
    def workingStatusMappingFunction: UserDefinedFunction = {
      udf((telfs: Double) => {
        if ((1 <= telfs) && (telfs < 3)) "working" else "not working"
      })
    }

    val workingStatusProjection: Column = workingStatusMappingFunction(df.col("telfs")).alias(WORKING)

    /** sex
      * - sex: value computed from the “tesex” column of the given DataFrame:
      *   - "male" if tesex = 1, "female" otherwise
      */
    def sexMappingFunction: UserDefinedFunction = {
      udf((tesex: Double) => {
        if (tesex == 1) "male" else "female"
      })
    }

    val sexProjection: Column = sexMappingFunction(df("tesex")).alias(SEX)

    /** age
      * - age: value computed from the “teage” column of the given DataFrame:
      *- "young" if 15 <= teage <= 22,
      *- "active" if 23 <= teage <= 55,
      *- "elder" otherwise
      */

    def ageMappingFunction: UserDefinedFunction = {
      udf((teage: Double) => {
        if ((15 <= teage) && (teage <= 22)) "young"
        else if ((23 <= teage) && (teage <= 55)) "active"
        else "elder"
      })
    }

    val ageProjection: Column = ageMappingFunction(df("teage")).alias(AGE)


    def columnsAdded(df: DataFrame, colNames: List[Column]): Column = colNames match {
      case Nil => new Column("")
      case column :: Nil => column
      case column :: tail => column + columnsAdded(df, tail)
    }

    val minutesInHour = 60
    val primaryNeedsProjection: Column = (columnsAdded(df, primaryNeedsColumns) / minutesInHour).alias(NEEDS)
    val workProjection: Column = (columnsAdded(df, workColumns) / minutesInHour).alias(WORK)
    val otherProjection: Column = (columnsAdded(df, otherColumns) / minutesInHour).alias(OTHER)

    /**
      * columnNames: List(tucaseid, gemetsta, gtmetsta, peeduca, pehspnon, ptdtrace, teage, telfs, temjot, teschenr, teschlvl, tesex, tespempnot, trchildnum, trdpftpt, trernwa, trholiday, trspftpt, trsppres, tryhhchild, tudiaryday, tufnwgtp, tehruslt, tuyear, t010101, t010102, t010199, t010201, t010299, t010301, t010399, t010401, t010499, t010501, t010599, t019999, t020101, t020102, t020103, t020104, t020199, t020201, t020202, t020203, t020299, t020301, t020302, t020303, t020399, t020401, t020402, t020499, t020501, t020502, t020599, t020681, t020699, t020701, t020799, t020801, t020899, t020901, t020902, t020903, t020904, t020905, t020999, t029999, t030101, t030102, t030103, t030104, t030105, t030108, t030109, t030110, t030111, t030112, t030186, t030199, t030201, t030202, t030203, t030204, t030299, t030301, t030302, t030303, t030399, t030401, t030402, t030403, t030404, t030405, t030499, t030501, t030502, t030503, t030504, t030599, t039999, t040101, t040102, t040103, t040104, t040105, t040108, t040109, t040110, t040111, t040112, t040186, t040199, t040201, t040202, t040203, t040204, t040299, t040301, t040302, t040303, t040399, t040401, t040402, t040403, t040404, t040405, t040499, t040501, t040502, t040503, t040504, t040505, t040506, t040507, t040508, t040599, t049999, t050101, t050102, t050103, t050189, t050201, t050202, t050203, t050204, t050289, t050301, t050302, t050303, t050304, t050389, t050403, t050404, t050405, t050481, t050499, t059999, t060101, t060102, t060103, t060104, t060199, t060201, t060202, t060203, t060289, t060301, t060302, t060303, t060399, t060401, t060402, t060403, t060499, t069999, t070101, t070102, t070103, t070104, t070105, t070199, t070201, t070299, t070301, t070399, t079999, t080101, t080102, t080199, t080201, t080202, t080203, t080299, t080301, t080302, t080399, t080401, t080402, t080403, t080499, t080501, t080502, t080599, t080601, t080602, t080699, t080701, t080702, t080799, t080801, t080899, t089999, t090101, t090102, t090103, t090104, t090199, t090201, t090202, t090299, t090301, t090302, t090399, t090401, t090402, t090499, t090501, t090502, t090599, t099999, t100101, t100102, t100103, t100199, t100201, t100299, t100381, t100383, t100399, t100401, t100499, t109999, t110101, t110199, t110281, t110289, t119999, t120101, t120199, t120201, t120202, t120299, t120301, t120302, t120303, t120304, t120305, t120306, t120307, t120308, t120309, t120310, t120311, t120312, t120313, t120399, t120401, t120402, t120403, t120404, t120405, t120499, t120501, t120502, t120503, t120504, t120599, t129999, t130101, t130102, t130103, t130104, t130105, t130106, t130107, t130108, t130109, t130110, t130111, t130112, t130113, t130114, t130115, t130116, t130117, t130118, t130119, t130120, t130121, t130122, t130123, t130124, t130125, t130126, t130127, t130128, t130129, t130130, t130131, t130132, t130133, t130134, t130135, t130136, t130199, t130201, t130202, t130203, t130204, t130205, t130206, t130207, t130208, t130209, t130210, t130211, t130212, t130213, t130214, t130215, t130216, t130217, t130218, t130219, t130220, t130221, t130222, t130223, t130224, t130225, t130226, t130227, t130228, t130229, t130230, t130231, t130232, t130299, t130301, t130302, t130399, t130401, t130402, t130499, t139999, t140101, t140102, t140103, t140104, t140105, t149999, t150101, t150102, t150103, t150104, t150105, t150106, t150199, t150201, t150202, t150203, t150204, t150299, t150301, t150302, t150399, t150401, t150402, t150499, t150501, t150599, t150601, t150602, t150699, t159989, t160101, t160102, t160103, t160104, t160105, t160106, t160107, t160108, t169989, t180101, t180199, t180280, t180381, t180382, t180399, t180481, t180482, t180499, t180501, t180502, t180589, t180601, t180682, t180699, t180701, t180782, t180801, t180802, t180803, t180804, t180805, t180806, t180807, t180899, t180901, t180902, t180903, t180904, t180905, t180999, t181002, t181081, t181099, t181101, t181199, t181201, t181202, t181204, t181283, t181299, t181301, t181302, t181399, t181401, t181499, t181501, t181599, t181601, t181699, t181801, t181899, t189999, t500101, t500103, t500104, t500105, t500106, t500107, t509989)
      */

    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force

  }

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    *               The resulting DataFrame should have the following columns:
    * - working: the “working” column of the `summed` DataFrame,
    * - sex: the “sex” column of the `summed` DataFrame,
    * - age: the “age” column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
    *               status, sex and age, rounded with a scale of 1 (using the `round` function),
    * - work: the average value of the “work” columns of all the people that have the same working status, sex
    *               and age, rounded with a scale of 1 (using the `round` function),
    * - other: the average value of the “other” columns all the people that have the same working status, sex and
    *               age, rounded with a scale of 1 (using the `round` function).
    *
    *               Finally, the resulting DataFrame should be sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame = {
    val df = summed.groupBy(WORKING, SEX, AGE).avg(NEEDS, WORK, OTHER)
      .orderBy(WORKING, SEX, AGE)

    //    println("timeUsageGrouped - start")
    //    df.show(5)
    //    println("timeUsageGrouped - stop")
    df
      .withColumn(NEEDS_AVG, round(df(NEEDS_AVG),1))
      .withColumn(WORK_AVG, round(df(WORK_AVG),1))
      .withColumn(OTHER_AVG, round(df(OTHER_AVG),1))
  }

  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    val df = spark.sql(timeUsageGroupedSqlQuery(viewName))
    //    println("timeUsageGroupedSql --------------------  start")
    //    df.show(5)
    //    println("timeUsageGroupedSql --------------------  stop")
    df
  }

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String = {
    "SELECT " + WORKING + ", " + SEX + ", " + AGE + ", " +
      " ROUND(AVG(" + NEEDS + "),1), ROUND(AVG(" + WORK + "),1), ROUND(AVG(" + OTHER + "),1) FROM " + viewName +
      " GROUP BY " + WORKING + ", " + SEX + ", " + AGE +
      " ORDER BY " + WORKING + ", " + SEX + ", " + AGE
  }

  /**
    * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    *
    *                           Hint: you should use the `getAs` method of `Row` to look up columns and
    *                           cast them at the same time.
    */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
    timeUsageSummaryDf.as[TimeUsageRow]


  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    *               Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    *               dataset contains one element per respondent, whereas the resulting dataset
    *               contains one element per group (whose time spent on each activity kind has
    *               been aggregated).
    *
    *               Hint: you should use the `groupByKey` and `typed.avg` methods.
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {

    // an action: summed.count()
    val so = summed.groupByKey(r => (r.working, r.sex, r.age)).agg(
      typed.avg((t: TimeUsageRow) => t.primaryNeeds).as[Double],
      typed.avg((t: TimeUsageRow) => t.work).as[Double],
      typed.avg((t: TimeUsageRow) => t.other).as[Double])
      .map(t => new TimeUsageRow(t._1._1, t._1._2, t._1._3, t._2, t._3, t._4))
      .orderBy(WORKING, SEX, AGE)
    so
      .withColumn(NEEDS, round(so(NEEDS),1))
      .withColumn(WORK, round(so(WORK),1))
      .withColumn(OTHER, round(so(OTHER),1))
      .as[TimeUsageRow]
  }
}

/**
  * Models a row of the summarized data set
  *
  * @param working      Working status (either "working" or "not working")
  * @param sex          Sex (either "male" or "female")
  * @param age          Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work         Number of daily hours spent on work
  * @param other        Number of daily hours spent on other activities
  */
case class TimeUsageRow(
                         working: String,
                         sex: String,
                         age: String,
                         primaryNeeds: Double,
                         work: Double,
                         other: Double
                       )


