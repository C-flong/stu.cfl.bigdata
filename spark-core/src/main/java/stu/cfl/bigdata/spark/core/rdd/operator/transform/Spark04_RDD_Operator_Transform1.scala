package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 flatMap
    val rdd: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val flatmapRDD: RDD[String] = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )

//    flatmapRDD.collect().foreach(println)
    println(flatmapRDD.collect().mkString(","))
    sc.stop()
  }
}
