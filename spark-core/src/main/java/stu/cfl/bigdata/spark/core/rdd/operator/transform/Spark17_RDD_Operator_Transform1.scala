package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key, Value)类型数据
        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3), ("a", 4)
        ),2)

        rdd.foldByKey(0)(_+_).collect().foreach(println)
        sc.stop()

    }
}
