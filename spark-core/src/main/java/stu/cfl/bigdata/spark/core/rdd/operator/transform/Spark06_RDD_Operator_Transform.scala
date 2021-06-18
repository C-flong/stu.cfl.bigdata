package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 groupby
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
    groupByRDD.collect().foreach(println)

    sc.stop()
  }
}
