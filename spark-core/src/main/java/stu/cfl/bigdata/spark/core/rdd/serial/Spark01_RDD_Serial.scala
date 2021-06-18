package stu.cfl.bigdata.spark.core.rdd.serial

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Serial {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("serial")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("h")
    search.getMatch1(rdd).collect().foreach(println)
    sc.stop()
  }

  class Search(query: String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(this.query)
    }

    // 函数序列化案例
    def getMatch1 (rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val s = query
      rdd.filter(x => x.contains(s))
    }
  }
}
