package stu.cfl.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 groupby
    val rdd: RDD[String] = sc.makeRDD(List("hello", "scala", "spark"))
    val groupByRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
    groupByRDD.collect().foreach(println)

    sc.stop()
  }
}
