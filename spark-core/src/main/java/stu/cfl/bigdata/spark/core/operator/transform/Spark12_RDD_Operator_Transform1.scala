package stu.cfl.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - sortBy
        val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("1", 1), ("2", 2), ("11", 11)), 2)
        val sortRDD = rdd.sortBy(_._1)
//        sortRDD.saveAsTextFile("output")
        rdd.collect().foreach(println)
        println("----------排序后-----------")
        sortRDD.collect().foreach(println)






        sc.stop()

    }
}
