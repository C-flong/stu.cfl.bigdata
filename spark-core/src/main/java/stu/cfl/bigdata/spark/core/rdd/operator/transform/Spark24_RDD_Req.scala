package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {

    def main(agrs: Array[String]): Unit = {

        // TODO 案例实操

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("requirement")
        val sc = new SparkContext(sparkConf)

        // 获取原始数据
        val dataRDD = sc.textFile("datas/agent.log")

        // 清洗并转换，((省份, 广告), 1)
        val mapRDD = dataRDD.map(
            line => {
                val datas = line.split(" ")
                // 元组用._1，数组用(1)
                ((datas(1), datas(4)), 1)
            }
        )
        // 聚合
        val reduceRDD = mapRDD.reduceByKey(_ + _)

        // 结构转换 (省份, (广告, n))
        val mapRDD1 = reduceRDD.map {
            case ((s, g), n) => {
                (s, (g, n))
            }
        }

        // 分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()

        // 组内排序，根据数量排序（降）
        val resultRDD = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )

        // 输出
        resultRDD.collect().foreach(println)
    }
}
