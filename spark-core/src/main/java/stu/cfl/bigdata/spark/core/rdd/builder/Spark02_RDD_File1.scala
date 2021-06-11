package stu.cfl.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD  从文件中创建
    // 可以指定具体文件也可以指定目录
    val rdd = sc.wholeTextFiles("datas")  // 以文件为单位(文件地址，文件内容)


    rdd.collect().foreach(println)
//    rdd.foreach(println)

    // TODO 关闭
    sc.stop()

  }


}
