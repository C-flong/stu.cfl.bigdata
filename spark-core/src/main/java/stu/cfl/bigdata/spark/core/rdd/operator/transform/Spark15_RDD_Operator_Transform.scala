package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key, Value)类型数据
        // reduceByKey : 相同的key的数据进行value数据的聚合操作
        // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
        // 【1，2，3】
        // 【3，3】
        // 【6】
        // reduceByKey中如果key的数据只有一个，是不会参与运算的。
        val rdd = sc.makeRDD(List(("1", 1), ("1", 11), ("1", 111), ("2", 2)))
        val reduceByKeyRDD = rdd.reduceByKey(
            (x, y) => {
                println(s"x=${x} y=${y}")
                x + y
            }
        )

        reduceByKeyRDD.collect().foreach(println)

        sc.stop()

    }
}
