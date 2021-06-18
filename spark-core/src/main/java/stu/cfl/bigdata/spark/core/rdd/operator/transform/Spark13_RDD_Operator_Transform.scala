package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - 双value类型的数据
        // 交集，并集和差集要求两个数据源数据类型保持一致
        // 拉链操作两个数据源的类型可以不一致
        // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
        // 两个数据源要求分区数量要保持一致
        // Can only zip RDDs with same number of elements in each partition
        // 两个数据源要求分区中数据数量保持一致
        val rdd1 = sc.makeRDD(List(1, 2, 5, 6))
        val rdd2 = sc.makeRDD(List(1, 2, 3, 4))

        val intersectionRDD = rdd1.intersection(rdd2)
        val unionRDD = rdd1.union(rdd2)
        val subtractRDD = rdd1.subtract(rdd2)
        val zipRDD = rdd1.zip(rdd2)

        println("交：" + intersectionRDD.collect().mkString(","))
        println("并：" + unionRDD.collect().mkString(","))
        println("差：" + subtractRDD.collect().mkString(","))
        println("拉链：" + zipRDD.collect().mkString(","))






        sc.stop()

    }
}
