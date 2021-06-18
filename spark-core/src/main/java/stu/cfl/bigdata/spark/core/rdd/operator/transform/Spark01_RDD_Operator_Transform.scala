package stu.cfl.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 map
    val rdd = sc.makeRDD(List[Int](1, 2, 3, 4))
//    rdd.map((n:Int)=>n*2)
//    rdd.map((n=>n*2))  // 类型可自行推断出来，参数可删除
    val mapRDD = rdd.map(_ * 2)  // 形参只出现一次，可用_代替

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
