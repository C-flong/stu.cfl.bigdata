package stu.cfl.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key, Value)类型数据
        // groupByKey : 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
        //              元组中的第一个元素就是key，
        //              元组中的第二个元素就是相同key的value的集合
        val rdd = sc.makeRDD(List(("1", 1), ("1", 11), ("1", 111), ("2", 2)))
        val groupByKeyRDD = rdd.groupByKey()
        groupByKeyRDD.collect().foreach(println)

        val groupByRDD = rdd.groupBy(_._1)
        groupByRDD.collect().foreach(println)
        sc.stop()

    }
}
