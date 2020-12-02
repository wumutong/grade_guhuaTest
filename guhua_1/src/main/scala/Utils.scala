package Scala

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.DataFrame


object Utils {


  val dbType = "Hive"
  // 读取配置文件
//  private val props = new Properties()
//  props.load(new FileInputStream("application.properties"))
//  // 服务参数配置
//  private val url: String = props.getProperty("mysql.url")
//  private val user: String = props.getProperty("mysql.user")
//  private val password: String = props.getProperty("mysql.password")
//
//  def write(df: DataFrame, dbtable: String, url: String = url, user: String = user, password: String = password) = {
//    df.write.mode("Append").format("jdbc").options(
//      Map(
//        "driver" -> "com.mysql.jdbc.Driver",
//        "url" -> url,
//        "user" -> user,
//        "password" -> password,
//        "dbtable" -> dbtable,
//        "batchsize" -> "2000",
//        "truncate" -> "false")).save()
//  }

  def readFromTxtByLine(filePath: String) = {
    //导入Scala的IO包
    import scala.io.Source
    //以指定的UTF-8字符集读取文件，第一个参数可以是字符串或者是java.io.File
    val source = Source.fromFile(filePath, "UTF-8")
    //将所有行放到数组中
    val lines = source.getLines().toArray
    source.close()
    //println(lines.size)
    lines
  }

  def arrayAllContains(array:Array[String],char:String) ={
    var j = 0
    var tag = false

    for(i <- 0 until array.length - 1){
      if(array(i).contains(char)){
        j = j + 1
      }
    }

    if(j == array.length) {
      tag = true
    }

    tag
  }

  def arrayPartContains(array:Array[String],char:String) ={
    var j = 0
    var tag = false

    for(i <- 0 until array.length - 1){
      if(array(i).contains(char)){
        j = j + 1
      }
    }

    if(j < array.length && j > 0) {
      tag = true
    }
    tag
  }

  def arrayNoContains(array:Array[String],char:String) ={
    var j = 0
    var tag = false

    for(i <- 0 until array.length - 1){
      if(array(i).contains(char)){
        j = j + 1
      }
    }

    if(j == 0) {
      tag = true
    }

    tag
  }



}
