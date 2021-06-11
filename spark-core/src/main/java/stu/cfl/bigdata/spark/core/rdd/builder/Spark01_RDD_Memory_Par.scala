package stu.cfl.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
//    sparkConf.set("spark.default.parallelism", "5")  // 显式设置并行度

    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD  并行度 & 分区
    val seq = Seq[Int](1, 2, 3, 4)
    // 方法1
//    val rdd = sc.parallelize(seq)
    // 方法2
    val rdd = sc.makeRDD(seq, 3)
    // 第二个参数表示分区数量，默认值表示机器可用核数
    // 分区数据分配
    // case _ =>
    //   val array = seq.toArray // To prevent O(n^2) operations for List etc
    //   positions(array.length, numSlices).map { case (start, end) =>
    //      array.slice(start, end).toSeq
    //   }.toSeq

    rdd.saveAsTextFile("output")

    // TODO 关闭
    sc.stop()

  }


}
