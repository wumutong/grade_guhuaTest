package Scala


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}




object guhuaTest {
  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val conf = new SparkConf(true)
//      .setAppName("guhua")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val spark = SparkSession
//      .builder()
//      .config(conf)
//      .enableHiveSupport()
//      .getOrCreate()
//    import spark.implicits._
//    val sc = spark.sparkContext
//
//    //获取指定路径
//    val paths = args(0).split(",")(0)
//
//    //加载相关数据 签名+通道号 用于生成指定相关条件
//    val hdfsText = sc.textFile(paths).collect()
//
//    //获取指定相关时间范围
//    val start_date = args(0).split(",")(1)
//    val end_date = args(0).split(",")(2)
//
//    val tableName = args(0).split(",")(3)

    val hdfsText = Utils.readFromTxtByLine("/Users/wumutong/conf/num_sign.txt")
    //生成指定sql
    val concatSql = formatSql(hdfsText, "num", "sign_sha256","20210301","20210331","logstash.dws_pubdefault_s_sms_day")

    println(concatSql)

//    //写入到指定hive表
//    val guaHuaDF: DataFrame = spark.sql(concatSql)
//
//    //生成指定文件目录
//    val months = start_date.substring(0,6)
//
//    guaHuaDF.write.orc("/tmp/settlement/yellowpage_onlinepub/dm_OnlineAndYellowPage_pub/months="+months)
  }




  // 根据场景 拼写相关sql
  def formatSql(key: Array[String], numName: String, sign_nameName: String,startDate:String,endDate:String,tableName:String): String = {

    var formatSql = "select step3.sign_sha256,step3.brand,count(step3.act_uv) as act_uv, count(step3.imei_sha256 ) as act_all " +
                    "from( " +
                      "select sign_sha256,brand,CASE WHEN ac>0 THEN imei_sha256 ELSE NULL end as act_uv, imei_sha256 " +
                    "from " +
                     tableName +
                    " where " +
                    " stat_date >= " + startDate + " and stat_date <= "+endDate +
                    " and ("

    for (i <- 0 until key.length) {
      var lines = key(i).split("\t")
      //  lines(0) 代表是 通道号   lines(1) 代表是签名
      var nums = lines(0).split(",")
      var sign_names = lines(1).split(",")

      //通道号相关 sql 拼接格式
      for (j <- 0 until nums.length) {

        //只有单个通道号情况下 ： xxxxxxxx*  或者 xxxxxxxxxx
        if (nums.length == 1) {
          if (nums(j).contains("*"))
            formatSql += "(" + numName + " like \"" + nums(j).replace("*", "%") + "\" and "
          else
            formatSql += "(" + numName + "= \"" + nums(j) + "\" and "
        }



        //有多个通道号情况下 ： xxxxxx*,xxxxxx*  或者 xxxxxxxx,xxxxx*xxxxx,xxxxxxxx 或者 xxxxxxxxxxxx,xxxxxxxxx
        if (nums.length >= 2) {

          //正常情况下  当通道号都包括*   比如： xxxxxxxxx*,xxxxxx*
          if (Utils.arrayAllContains(nums, "*")) {

            if (nums(j).contains("*") && j == 0) {
              formatSql += "(( " + numName + " like \"" + nums(j).replace("*", "%") + "\" or "
            } else if (nums(j).contains("*") && j > 0 && j != nums.length - 1) {
              formatSql += numName + " like \"" + nums(j).replace("*", "%") + "\" or "
            } else if (nums(j).contains("*") && j == nums.length - 1) {
              formatSql += numName + " like \"" + nums(j).replace("*", "%") + "\" ) and "
            }

          }

          //如果部分包括 *    比如：xxxxxxxxx,xxxxxxxx* ,或者 xxxxxx*,xxxxx,xxxxxx*
          if (Utils.arrayPartContains(nums, "*")) {
            if (nums(j).contains("*") && j == 0) {
              formatSql += "(( " + numName + " like \"" + nums(j).replace("*", "%") + "\" or "
            } else if (j == 0) {
              formatSql += "(( " + numName + "= \"" + nums(j) + "\" or "
            } else if (nums(j).contains("*") && j > 0 && j != nums.length - 1) {
              formatSql += numName + " like \"" + nums(j).replace("*", "%") + "\" or "
            } else if (j > 0 && j != nums.length - 1) {
              formatSql += numName + "= \"" + nums(j) + "\" or "
            } else if (nums(j).contains("*") && j == nums.length - 1) {
              formatSql += numName + " like \"" + nums(j).replace("*", "%") + "\" ) and "
            } else if (j == nums.length - 1) {
              formatSql += numName + "= \"" + nums(j) + "\" ) and "
            }
          }

          //如果 都不包括 * 比如 ：xxxxxxxxxxxx,xxxxxxxxx

          if (Utils.arrayNoContains(nums, "*")) {

            if (j == 0) {
              formatSql += "(( " + numName + "= \"" + nums(j) + "\" or "
            } else if (j > 0 && j != nums.length - 1) {
              formatSql += numName + " = \"" + nums(j) + "\" or "
            } else if (j == nums.length - 1) {
              formatSql += numName + " = \"" + nums(j) + "\" ) and "
            }

          }
        }

      }
      //签名相关格式 拼写
      for (j <- 0 until sign_names.length) {
        if (sign_names.length == 1) {
          formatSql += sign_nameName + " like concat ( \"%\","+ "sha2(\"" + sign_names(j) + "\",256),\"%\"))"
        } else if (sign_names.length > 1 && j == 0) {
          formatSql += "(" + sign_nameName + " like concat ( \"%\","+  "sha2(\"" + sign_names(j) + "\",256) ,\"%\") or "
        } else if (sign_names.length > 1 && j < sign_names.length - 1) {
          formatSql += sign_nameName + " like  concat ( \"%\","+  "sha2(\"" + sign_names(j) + "\",256),\"%\") or "
        } else if (sign_names.length > 1 && j == sign_names.length - 1) {
          formatSql += sign_nameName + " like  concat ( \"%\","+ "sha2(\"" + sign_names(j) + "\",256),\"%\")))"
        }
      }

      if (i < key.length - 1) {
        formatSql += "\r\n or "
      }
    }


    formatSql += ")group by " +
      "sign_sha256," +
      "brand," +
      "CASE WHEN ac>0 THEN imei_sha256 ELSE NULL end, " +
      "imei_sha256 )step3 " +
      "group by " +
      "step3.sign_sha256,step3.brand"
    formatSql

  }


}





























