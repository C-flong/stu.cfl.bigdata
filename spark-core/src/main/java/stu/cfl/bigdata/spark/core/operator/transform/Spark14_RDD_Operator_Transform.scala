package stu.cfl.bigdata.spark.core.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key, Value)类型数据
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
        val mapRDD = rdd.map((_, Nil))
        // RDD => PairRDDFunctions
        // 隐式转换（二次编译）

        // partitionBy根据指定的分区规则对数据进行重分区
        mapRDD.partitionBy(new HashPartitioner(2))

        mapRDD.saveAsTextFile("output")

        sc.stop()

    }
}
