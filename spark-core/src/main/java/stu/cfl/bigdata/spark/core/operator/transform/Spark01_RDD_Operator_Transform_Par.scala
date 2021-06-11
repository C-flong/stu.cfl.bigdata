package stu.cfl.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 map

    // 1. rdd的计算一个分区内的数据是一个一个执行逻辑
    //    只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。
    //    分区内数据的执行是有序的。
    // 2. 不同分区数据计算是无序的。

    val rdd = sc.makeRDD(List[Int](1, 2, 3, 4))
//    rdd.map((n:Int)=>n*2)
//    rdd.map((n=>n*2))  // 类型可自行推断出来，参数可删除
    val mapRDD = rdd.map(
      (n: Int) => {
        println("111111111")
        n * 2
      }
    )  // 形参只出现一次，可用_代替

    val mapRDD2 = mapRDD.map(
      (n: Int) => {
        println("222222222")
        n * 2
      }
    )  // 形参只出现一次，可用_代替

    mapRDD2.collect()

    sc.stop()
  }
}
