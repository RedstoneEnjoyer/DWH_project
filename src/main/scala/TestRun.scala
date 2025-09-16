import org.apache.spark.api.java.Optional
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

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

  if (1 == 0) {
    println("BANK")
    bankRDD.take(3).foreach(println)
    println("MARKET")
    marketRDD.take(3).foreach(println)
    println("INSURANCE")
    insuranceRDD.take(3).foreach(println)
  }

//  Функции нормализации полей
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
    val emailRegex = """^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$""".r
    Some(email).filter(e => emailRegex.pattern.matcher(e).matches)
  }

  // вход: RDD[(systemId, clientId, row, index)]
  // выход: RDD[(systemId, clientId, fioOpt, document, phone0, phone1, phone3, emailOpt, dr, index)]
  def normalizeData(input: RDD[(Int, Int, Row, Long)]):RDD[(Int, Int, Option[String], String, String, String, String, Option[String], String, Long)] = {
    input.map { case (systemId, clientId, row, index) =>
      val fioOpt = normalizeFio(row.getAs[String]("fio"))

      val serialNorm = normalizeDocument(row.getAs[String]("serial_number"))
      val innNorm = normalizeDocument(row.getAs[String]("inn"))
      val document = if (serialNorm.nonEmpty) serialNorm else innNorm

      val phone0 = normalizePhone(row.getAs[String]("phone0"))
      val phone1 = normalizePhone(row.getAs[String]("phone1"))
      val phone3 = normalizePhone(row.getAs[String]("phone3"))

      val emailOpt = validateEmail(row.getAs[String]("email"))
      val dr = Option(row.getAs[String]("dr")).getOrElse("")

      (systemId, clientId, fioOpt, document, phone0, phone1, phone3, emailOpt, dr, index)
    }
  }

  val normalizedBank = normalizeData(bankRDD)
  val normalizedInsurance = normalizeData(insuranceRDD)
  val normalizedMarket = normalizeData(marketRDD)

  val commonData = normalizedBank.union(normalizedInsurance).union(normalizedMarket)

//  Правила метчинга

//  Утилита для создания ключа (типа хэша)
  def createKey(fields: List[String]): String = {
    fields.filter(_.nonEmpty).mkString("|")
  }
//  Утилита для извлечения значений полей из записи
  def extractFields(record: (Int, Int, Option[String], String, String, String, String, Option[String], String, Long),
                    fieldNames: List[String]): List[String] = {
    val (systemId, clientId, fioOpt, document, phone0, phone1, phone3, emailOpt, dr, index) = record

    fieldNames.map {
      case "fio" => fioOpt.getOrElse("")
      case "document" => document
      case "phone" => List(phone0, phone1, phone3).find(_.nonEmpty).getOrElse("")
      case "email" => emailOpt.getOrElse("")
      case "dr" => dr
      case _ => ""
    }
  }
//  Правила матчинга в общем виде
  case class MatchingRule(id: Int, weight: Int, fields: List[String])

  val bankInsuranceRules = List(
    MatchingRule(1, 80, List("fio", "phone", "dr", "document")), // так как флага телефона нет взял наименьшый вес
    MatchingRule(2, 70, List("fio", "email", "dr", "document")),
    MatchingRule(3, 60, List("fio", "dr", "document"))
  )

  val bankMarketRules = List(
    MatchingRule(1, 80, List("fio", "phone", "email")), // тоже нет флага
    MatchingRule(2, 70, List("fio", "email"))
  )

//  Функция метчинга (Банк + другая система)
  def performMatching(bankData: RDD[(Int, Int, Option[String], String, String, String, String, Option[String], String, Long)],
                      otherData: RDD[(Int, Int, Option[String], String, String, String, String, Option[String], String, Long)],
                      rules: List[MatchingRule]): RDD[((Int, Int), (Int, Int), Int, Int)] = {

    val allMatches = rules.flatMap { rule =>
//      Создаем ключи для банковских данных
      val bankKeys = bankData.map { record =>
        val fields = extractFields(record, rule.fields)
        val key = createKey(fields)
        (key, (record._1, record._2)) // (key, (systemId, clientId))
      }.filter(_._1.nonEmpty)

//      Создаем ключи для других данных
      val otherKeys = otherData.map { record =>
        val fields = extractFields(record, rule.fields)
        val key = createKey(fields)
        (key, (record._1, record._2)) // (key, (systemId, clientId))
      }.filter(_._1.nonEmpty)

//      Джойним по ключам
      val matches = bankKeys.join(otherKeys)
        .map { case (key, (bankClient, otherClient)) =>
          (bankClient, otherClient, rule.id, rule.weight)
        }

      matches.collect()
    }
//    Переводим в RDD и удаляем дубликаты
    sc.parallelize(allMatches)
      .map { case (bankClient, otherClient, ruleId, weight) =>
        ((bankClient, otherClient), (ruleId, weight))
      }
      .reduceByKey { (tuple1: (Int, Int), tuple2: (Int, Int)) =>
        val (ruleId1, weight1) = tuple1
        val (ruleId2, weight2) = tuple2
        if (weight1 >= weight2) (ruleId1, weight1) else (ruleId2, weight2)
      }
      .map { case ((bankClient, otherClient), (ruleId, weight)) =>
        (bankClient, otherClient, ruleId, weight)
      }
  }

//  Метчинг трех систем попарно:
  val bankInsuranceMatches = performMatching(normalizedBank, normalizedInsurance, bankInsuranceRules)
  val bankMarketMatches = performMatching(normalizedBank, normalizedMarket, bankMarketRules)

  println("BANK - INSURANCE")
  bankInsuranceMatches.foreach(println)

  println("BANK - MARKET")
  bankMarketMatches.foreach(println)
  println("end message")
}