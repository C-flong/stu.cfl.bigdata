package stu.cfl.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        // 获取系统累加器
        // Spark默认提供了简单数据聚合的累加器
        val sumAcc = sc.longAccumulator("sum")

        // 其他累加器
        //sc.doubleAccumulator
        //sc.collectionAccumulator

        rdd.foreach(
            num => {
                sumAcc.add(num)
            }
        )

        print(sumAcc)
        sc.stop()

    }
}
