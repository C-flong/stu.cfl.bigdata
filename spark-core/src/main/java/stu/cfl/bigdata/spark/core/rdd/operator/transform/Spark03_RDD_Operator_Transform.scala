package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 mapPartitionsWithIndex
    val rdd = sc.makeRDD(List[Int](1, 2, 3, 4), 2)
    val mapPWIRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        }
        else {
          Nil.iterator
        }
      }
    )
    mapPWIRDD.collect().foreach(println)

    sc.stop()
  }
}
