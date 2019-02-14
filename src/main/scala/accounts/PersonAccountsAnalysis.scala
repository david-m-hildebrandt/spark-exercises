package accounts

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object AccountType extends Enumeration {
  type AccountType = Value
  val SAVINGS, CHEQUING, CREDIT, BROKER = Value

  private val random = Random

  def getRandomAccounts: List[AccountType] = {
    val totalNumber = random.nextInt(3)
    var totalChosen = 0
    var randomAccounts: List[AccountType] = List()
    while (totalChosen < totalNumber) {
      for {
        at <- values
        if !randomAccounts.contains(at) && Random.nextBoolean()
      } {
        totalChosen = totalChosen + 1
        randomAccounts = at :: randomAccounts
      }
    }
    randomAccounts
  }

  def get(v: String): AccountType.AccountType = {
    values.filter(t => t.toString.equals(v)).head
  }
}


object PersonAccountsAnalysis {

  import AccountType._

  /*  val sparkSession: SparkSession =
      SparkSession
        .builder()
        .appName("Person Accounts Analysis")
        .config("spark.master", "local[*]")
        .getOrCreate()
  */
  val sparkConf: SparkConf = new SparkConf().setAppName("Person Accounts Analysis").setMaster("local[*]")
  val sparkContext: SparkContext = SparkContext.getOrCreate(sparkConf)

  /** Main function */
  def main(args: Array[String]): Unit = {

//    val accountRdd = sparkSession.sparkContext.textFile(fsPath("/accounts/account.csv"))
//    val personRdd = sparkSession.sparkContext.textFile(fsPath("/accounts/person.csv"))
    val accountRdd = sparkContext.textFile(fsPath("/accounts/account.csv"))
    val personRdd = sparkContext.textFile(fsPath("/accounts/person.csv"))

    println("account - start")
    val accountRawPairRdd = toAccountRawPairRdd(accountRdd).persist()
    val accountPairRdd = toAccountPairRdd(accountRdd).persist()
    println("accountRawPairRdd")
    accountRawPairRdd.collect().foreach(println(_))
    println("accountPairRdd")
    accountPairRdd.collect().foreach(println(_))
    println("account - end")

    println("person - start")
    val personRawPairRdd = toPersonRawPairRdd(personRdd).persist()
    val personPairRdd = toPersonPairRdd(personRdd).persist()
    println("personRawPairRdd")
    personRawPairRdd.collect().foreach(println(_))
    println("personPairRdd")
    personPairRdd.collect().foreach(println(_))
    println("person - end")

    println("personAccount - start")
    val personAccountRawPairRdd = personRawPairRdd.join(accountRawPairRdd)
    val personAccountPairRdd = personPairRdd.join(accountPairRdd)
    personAccountRawPairRdd.collect().foreach(println(_))
    personAccountPairRdd.collect().foreach(println(_))
    println("personAccount - end")

    // large savings, earnings above > 50,000
    val largeSavingsEarningsMoreThan50K = personAccountRawPairRdd.filter(largeSavingsEarningsMoreThan50KPredicate).persist()

    println("withMoney - start")
    largeSavingsEarningsMoreThan50K.foreach(println(_))
    println("withMoney - end")

    val brokerAccountCandidates = personAccountPairRdd
      .filter(earningsAtLeast(50000))
      .filter(ageAtLeast(45))
      .filter(ageAtMost(55))
      .filter(balanceAtLeast(10000)(AccountType.SAVINGS))
      .filter(hasNotAccountType(AccountType.BROKER))

    println("These people might want a broker")
    brokerAccountCandidates.collect().foreach(println(_))

    val creditAssitanceCandidates = personAccountPairRdd
      .filter(earningsAtMost(40000))
      .filter(ageAtLeast(18))
      .filter(ageAtMost(30))
      .filter(balanceAtMost(-10000)(AccountType.CREDIT))

    println("These people might need some credit assistance")
    creditAssitanceCandidates.collect().foreach(println(_))

    val chars = generateChars(100000)
    val charRdd = sparkContext.parallelize(chars)
    val totalChars = charRdd.count()
    println(s"Totalchars: ${totalChars}")
    val totalChars2 = charRdd.map(c => 1).sum()
    println(s"Totalchars: ${totalChars2.toInt}")

    val charDistributionRdd = charRdd.map(c => (c, 1)).groupByKey
    charDistributionRdd.collect().foreach(println)

    val charDistributionTotalRdd = charRdd.map(c => (c, 1)).reduceByKey((i, j) => i + j)
    charDistributionTotalRdd.collect().foreach(println)

    //    val cdUnion = charDistributionRdd.union(charDistributionTotalRdd)

  }


  def generateChars(total: Int): Seq[Char] = {
    val min = 32
    val max = 126
    val range = max - min
    (0 until total).map(i => (Random.nextInt(range) + min).toChar)
  }


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Raw Predicates
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def largeSavingsEarningsMoreThan50KPredicate(r: (String, ((String, Int, Int), Iterable[(String, AccountType.AccountType, Int)]))): Boolean = {

    val earnings: Int = r._2._1._3
    val savings: Option[Int] = try {
      Some(r._2._2.filter(v => AccountType.SAVINGS == v._2).head._3)
    } catch {
      case e: NoSuchElementException => None
    }

    savings match {
      case None => false
      case Some(s) => (earnings > 50000) && (s > 50000)
    }
  }


  def over40YearsPredicate(r: (String, ((String, Int, Int), Iterable[(String, AccountType, Int)]))): Boolean = {
    r._2._1._2 > 40
  }

  def hasNoBROKERPredicate(r: (String, ((String, Int, Int), Iterable[(String, AccountType.AccountType, Int)]))): Boolean = {
    r._2._2.forall(e => AccountType.BROKER == e._2)

  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Case Class Predicates
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private def balanceAtMostForBrokerAccount(measure: Int)(r: (String, (Person, Iterable[Account]))) =
    balanceAtMost(measure)(AccountType.BROKER)(r)

  private def balanceAtMost(measure: Int)(accountType: AccountType)(r: (String, (Person, Iterable[Account]))) =
    balanceQualifiesPredicate(lessThanOrEqual)(measure)(accountType)(r)

  private def balanceAtLeast(measure: Int)(accountType: AccountType)(r: (String, (Person, Iterable[Account]))) =
    balanceQualifiesPredicate(greaterThanOrEqual)(measure)(accountType)(r)

  def lessThan(a: Int, b: Int): Boolean = a < b

  def lessThanOrEqual(a: Int, b: Int): Boolean = a <= b

  def greaterThan(a: Int, b: Int): Boolean = a > b

  def greaterThanOrEqual(a: Int, b: Int): Boolean = a > b

  def balanceQualifiesPredicate(op: (Int, Int) => Boolean)
                               (measure: Int)
                               (accountType: AccountType)
                               (r: (String, (Person, Iterable[Account]))): Boolean = {

    val balance: Option[Int] = try {
      Some(r._2._2.filter(v => accountType == v.accountType).head.balance)
    } catch {
      case e: NoSuchElementException => None
    }

    balance match {
      case None => false
      case Some(b) => op(b, measure)
    }

  }

  private def atLeastAge50(r: (String, (Person, Iterable[Account]))) = ageAtLeast(50)(r)

  private def atMostAge40(r: (String, (Person, Iterable[Account]))) = ageAtMost(40)(r)

  def ageAtMost(age: Int)(r: (String, (Person, Iterable[Account]))): Boolean =
    r._2._1.age <= age

  def ageAtLeast(age: Int)(r: (String, (Person, Iterable[Account]))): Boolean =
    r._2._1.age >= age

  def earningsAtMost(measure: Int)(r: (String, (Person, Iterable[Account]))): Boolean =
    r._2._1.earnings <= measure

  def earningsAtLeast(measure: Int)(r: (String, (Person, Iterable[Account]))): Boolean =
    r._2._1.earnings >= measure

  def hasNotAccountType(accountType: AccountType)(r: (String, (Person, Iterable[Account]))): Boolean =
    !hasAccountType(accountType)(r)

  def hasAccountType(accountType: AccountType)(r: (String, (Person, Iterable[Account]))): Boolean =
    r._2._2.exists(e => accountType == e.accountType)


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Account
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def toAccountRawPairRdd(a: RDD[String]): RDD[(String, Iterable[(String, AccountType.AccountType, Int)])] = {
    // a(1) = personId
    // a(0) = accountId
    // a(2) = accountType
    // a(3) = accountBalance
    a.map(_.split(",")).map(a => (a(1), (a(0), AccountType.get(a(2)), a(3).toInt))).groupByKey()
  }

  case class Account(id: Int, personId: Int, accountType: AccountType, balance: Int) {
    override def toString = s"Account(id: $id personId: $personId accountType: $accountType balance $balance)"
  }

  def toAccount(a: Array[String]): Account =
    Account(a(0).toInt, a(1).toInt, AccountType.get(a(2)), a(3).toInt)


  def toAccountPairRdd(a: RDD[String]): RDD[(String, Iterable[Account])] =
    a.map(_.split(",")).map(a => (a(1), toAccount(a))).groupByKey()


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Person
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def toPersonRawPairRdd(a: RDD[String]): RDD[(String, (String, Int, Int))] = {
    // a(0) = personId
    // a(1) = personName
    // a(2) = personAge
    // a(3) = personEarnings
    a.map(_.split(",")).map(a => (a(0).trim(), (a(1).trim(), a(2).toInt, a(3).toInt)))
  }

  case class Person(id: String, name: String, age: Int, earnings: Int) {
    override def toString = s"Person(id: $id name: $name age: $age earnings $earnings)"
  }

  def toPerson(a: Array[String]): Person =
    Person(a(0).trim(), a(1).trim(), a(2).toInt, a(3).toInt)


  def toPersonPairRdd(a: RDD[String]): RDD[(String, Person)] =
    a.map(_.split(",")).map(a => (a(0), toPerson(a)))


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // File System Utility
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

}



