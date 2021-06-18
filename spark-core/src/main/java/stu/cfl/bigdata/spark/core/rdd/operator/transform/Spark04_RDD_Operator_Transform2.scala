package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 flatMap
    val rdd = sc.makeRDD(List(List(1, 2), 3, List(3, 4)))

    val flatmapRDD = rdd.flatMap(
      obj => {
        obj match{
          case list: List[_] => list.iterator
          case dat => List(dat)
        }
      }
    )

    flatmapRDD.collect().foreach(println)
    sc.stop()
  }
}
