package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)

        val rdd1 = sc.makeRDD(List(
            ("a", 1), ("b", 2)//, ("c", 3)
        ))

        val rdd2 = sc.makeRDD(List(
            ("a", 4), ("b", 5),("a", 6)
        ))

        val leftJoinRDD = rdd1.leftOuterJoin(rdd2)
        val rightJoinRDD = rdd1.rightOuterJoin(rdd2)

//        left
//        (a,(1,Some(4)))
//        (a,(1,Some(6)))
//        (b,(2,Some(5)))
//        right
//        (a,(Some(1),4))
//        (a,(Some(1),6))
//        (b,(Some(2),5))

        println("left")
        leftJoinRDD.collect().foreach(println)
        println("right")
        rightJoinRDD.collect().foreach(println)



        sc.stop()

    }
}
