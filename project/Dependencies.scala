import sbt._

object Dependencies {
  val library: Object {
    val sparkSql: ModuleID
  } = new {
    object Version {
      lazy val spark = "3.5.0"
    }
    val sparkSql = "org.apache.spark" %% "spark-sql" % Version.spark

  }
}
