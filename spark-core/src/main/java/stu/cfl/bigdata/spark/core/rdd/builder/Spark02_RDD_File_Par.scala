package stu.cfl.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD  从文件中创建
    // 可以指定具体文件也可以指定目录
    val rdd: RDD[String] = sc.textFile("datas/1.txt", 2)
    // 一行一行读(采取的读取方式和hadoop一样)
    // 第二个参数表示最小分区数（不一定是总分区数）
    // textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
    //     minPartitions : 最小分区数量
    //     math.min(defaultParallelism, 2)
    //val rdd = sc.textFile("datas/1.txt")
    // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    // Spark读取文件，底层其实使用的就是Hadoop的读取方式
    // 分区数量的计算方式：
    //    totalSize = 7
    //    goalSize =  7 / 2 = 3（byte）

    //    7 / 3 = 2...1 (1.1) + 1 = 3(分区)

    //
    rdd.saveAsTextFile("output")

    // TODO 关闭
    sc.stop()

  }


}
