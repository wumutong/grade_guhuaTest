package Scala
import org.apache.spark.sql.DataFrame

trait DataAccess {
  // xx
  val dbType: String
  val driverClass: String
  // xx
  def loadTable(table: String): DataFrame
  def sqlQuery(sql: String): DataFrame
}
