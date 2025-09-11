import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD

object TestRun extends App {
  val spark = SparkSession
    .builder()
    .appName("Курсовая работа Spark Developer")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  val sqlContext = spark.sqlContext

  import org.apache.spark.sql.functions._

  val sc = spark.sparkContext

  println("begin message")

//  Чтение синтетики
  val filepath = "./source.csv"
  val rawData = spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(filepath)

//  Делаем из датафрейма rdd + правильная индексация строк
  val dataRDD = rawData.rdd.zipWithIndex()
  val indexedRDD = dataRDD.map{ case(value, index) => (value, index + 1) }

//  Функции разбиения по источникам с учетом того что строки нумеруются с 0
  def isBank(index: Long): Boolean = {
    (index >= 1 && index <= 800) || index == 1001
  }

  def isInsurance(index: Long): Boolean = {
    (index >= 100 && index <= 300) ||
      (index >= 400 && index <= 600) ||
      (index >= 900 && index <= 1000) ||
      index == 1002 || index == 1006
  }

  def isMarket(index: Long): Boolean = {
    (index >= 800 && index <= 900) ||
      (index >= 200 && index <= 700) ||
      index == 1003 || index == 1004 || index == 1005 || index == 1007
  }

//  Разбиение на три RDD + номер источника + уникальность client_id
  val bankRDD = indexedRDD
    .filter { case (row, index) => isBank(index) }
    .map { case (row, index) =>
      val systemId = 1
      val clientId = row.getAs[String]("client_id").toInt
      (systemId, clientId, row, index)
    }
  val insuranceRDD = indexedRDD
    .filter { case (row, index) => isInsurance(index) }
    .map{ case (row, index) =>
      val systemId = 2
      val clientId = row.getAs[String]("client_id").toInt + 1500
      (systemId, clientId, row, index)
    }
  val marketRDD = indexedRDD
    .filter { case (row, index) => isMarket(index) }
    .map { case(row, index) =>
      val systemId = 3
      val clientId = row.getAs[String]("client_id").toInt + 3000
      (systemId, clientId, row, index)
    }

  if (1 == 1) {
    println("BANK")
    bankRDD.take(3).foreach(println)
    println("MARKET")
    marketRDD.take(3).foreach(println)
    println("INSURANCE")
    insuranceRDD.take(3).foreach(println)
  }

//  Функции нормализации данных
  def normalizePhone(phone: String): String = {
    if (phone == null) return ""
    phone.replaceAll("[^0-9]", "")
  }
  def normalizeFio(fio: String): Option[String] = {
    if (fio == null) return None
    val cleaned = fio.replaceAll("[^а-яёА-ЯЁ\\s]", "").replaceAll("\\s+", " ").trim
    val words = cleaned.split("\\s+")
    if (words.length == 3 && words.forall(_.nonEmpty)) Some(cleaned) else None
  }
  def normalizeDocument(doc: String): String = {
    if (doc == null) return ""
    doc.replaceAll("[^0-9]", "")
  }
  def validateEmail(email: String): Option[String] = {
    if (email == null) return None
    val pattern = """^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$""".r
    if (pattern.matches(email)) Some(email) else None
  }

//  val filtered = dataRDD.filter{case (value, index) => index == 0}
//  filtered.collect().foreach(println)

  println("end message")

}