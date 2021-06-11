package stu.cfl.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 flatMap
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    val flatmapRDD = rdd.flatMap(
      list => {
        list
      }
    )

    flatmapRDD.collect().foreach(println)
    sc.stop()
  }
}
