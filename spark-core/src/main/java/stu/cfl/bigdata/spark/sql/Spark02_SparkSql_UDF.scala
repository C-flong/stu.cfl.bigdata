package stu.cfl.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark02_SparkSql_UDF {
  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("datas/user.json")
    df.createTempView("user")
//    spark.sql("select concat('name:', username), age from user").show()
    spark.udf.register(
      "perfixName",
      "name:"+_
//      (name:String) => "name"+name
    )
    spark.sql("select perfixName(username), age from user").show()


    // TODO 关闭环境
    spark.close()
  }
  case class User( id:Int, name:String, age:Int )

}
