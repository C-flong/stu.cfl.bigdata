package stu.cfl.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - sortBy
        val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)
        val sortRDD = rdd.sortBy(n=>n)
        sortRDD.saveAsTextFile("output")
//        sortRDD.collect().foreach(println)






        sc.stop()

    }
}
