package accounts

import java.io.{File, PrintWriter}

import accounts.AccountType.AccountType

import scala.io.Source
import scala.util.Random


object DataGenerator {

  val random = Random

  val minAge = 18
  val maxAge = 60
  val n = 100

  def main(a: Array[String]): Unit = {
    createPersonData()
    createAccountData()
  }

  def createAccountData(): Unit = {

    val accounts = (0 to n).flatMap(i => AccountType.getRandomAccounts.map(a => (i, a)))

    val accountTupleList = accounts.zipWithIndex.map(v => (v._2, v._1._1, v._1._2, getUniformBalance(v._1._2)))

    val accountDataFile = new File("C:\\code\\lantern\\scala\\spark-exercises\\src\\main\\resources\\accounts\\account.csv")
    toAccountFile(accountDataFile, accountTupleList)

  }


  def createPersonData(): Unit = {
    val names = fromFile(new File("C:\\code\\lantern\\scala\\spark-exercises\\src\\main\\resources\\accounts\\names.csv"))

    val ages = getUniformAges(names.size)

    val idNameAgeList = names.zip(ages)

    val personTupleList = idNameAgeList.map(v => (v._1._1, v._1._2, v._2, getSalary(v._2)))

    print(personTupleList mkString ("\n"))

    val personDataFile = new File("C:\\code\\lantern\\scala\\spark-exercises\\src\\main\\resources\\accounts\\person.csv")
    toFile(personDataFile, personTupleList)

  }

  def getUniformBalance(at: AccountType): Int = {
    val randomFactor = random.nextInt(1000)
    at match {
      case AccountType.SAVINGS => 100 * randomFactor
      case AccountType.CHEQUING => 25 * randomFactor
      case AccountType.CREDIT => -25 * randomFactor
      case AccountType.BROKER => 200 * randomFactor
    }
  }


  def getUniformAges(n: Int): List[Int] = {
    (for (i <- 0 to n) yield {
      random.nextInt(maxAge - minAge) + minAge
    }).toList
  }

  def getSalary(age: Int): Int = {
    val (mean, sigma) = getMeanAndSigma(age)
    getGaussianSalary(mean, sigma)
  }

  def getGaussianSalary(mean: Int, sd: Int): Int = {
    ((random.nextGaussian() * sd) + mean).toInt
  }

  def getMeanAndSigma(age: Int): (Int, Int) = {

    val baseSalary = 18000
    val baseSigma = 100

    val adjustedAge = age - minAge

    val adjustmentToSalary = 3000
    val adjustmentToSigma = 2

    ((baseSalary + (adjustmentToSalary * adjustedAge)).toInt, (baseSigma * adjustmentToSigma * adjustedAge).toInt)

  }

  def readFromFile(f: File): Iterable[String] = {
    Source.fromFile(f).getLines().toIterable
  }


  def fromFile(f: File): Iterable[(String, String)] = {
    readFromFile(f).map(line => {
      val values = line.split(",")
      (values(0), values(1))
    })
  }

  def printToFile(f: File)(op: PrintWriter => Unit): Unit = {
    val p = new PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  def toFile(f: File, personData: Iterable[(String, String, Int, Int)]): Unit = {
    printToFile(f)(
      p => {
        personData.iterator.foreach(personRecord => {
          p.println(s"${personRecord._1},${personRecord._2},${personRecord._3},${personRecord._4}")
        })
      }
    )
  }

  def toAccountFile(f: File, accountData: Iterable[(Int, Int, AccountType.AccountType, Int)]): Unit = {
    printToFile(f)(
      p => {
        accountData.iterator.foreach(accountRecord => {
          p.println(s"${accountRecord._1},${accountRecord._2},${accountRecord._3},${accountRecord._4}")
        })
      }
    )
  }

}
