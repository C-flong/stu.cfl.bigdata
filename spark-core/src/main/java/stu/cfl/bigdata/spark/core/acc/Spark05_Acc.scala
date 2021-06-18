package stu.cfl.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Acc {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd1 = sc.makeRDD(List(
            ("a", 1),("b", 2),("c", 3)
        ), 2)
//        val rdd2 = sc.makeRDD(List(
//            ("a", 4),("b", 5),("a", 6)
//        ), 2)
        val map = mutable.Map(("a", 4),("b", 5),("c", 6))



        // join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
        //val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
        //joinRDD.collect().foreach(println)
        // (a, 1),    (b, 2),    (c, 3)
        // (a, (1,4)),(b, (2,5)),(c, (3,6))
        rdd1.map{
            case (k, v) => {
                val l = map.getOrElse(k, 0)
                (k, (v, l))
            }
        }.collect().foreach(println)


        sc.stop()

    }
}
