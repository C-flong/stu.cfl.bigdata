package stu.cfl.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD  从内存中创建
    val seq = Seq[Int](1, 2, 3, 4)
    // 方法1
//    val rdd = sc.parallelize(seq)
    // 方法2
    val rdd = sc.makeRDD(seq)

    rdd.collect().foreach(println)
//    rdd.foreach(println)

    // TODO 关闭
    sc.stop()

  }


}
