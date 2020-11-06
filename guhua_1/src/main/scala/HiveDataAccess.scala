package Scala
import org.apache.spark.sql.{DataFrame, SparkSession}


class HiveDataAccess(val spark: SparkSession) extends DataAccess {
  val dbType = "HIVE"
  val driverClass = "org.apache.hive.jdbc.HiveDriver"
  // xx
  def loadTable(table: String): DataFrame = spark.table(table)
  def sqlQuery(sql: String): DataFrame = spark.sql(sql)
}