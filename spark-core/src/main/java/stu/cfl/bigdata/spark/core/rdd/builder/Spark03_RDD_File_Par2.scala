package stu.cfl.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD  从文件中创建
    // 可以指定具体文件也可以指定目录
    val rdd: RDD[String] = sc.textFile("datas/word.txt", 2)
    // 一行一行读(采取的读取方式和hadoop一样)
    // 第二个参数表示最小分区数
    // TODO 数据分区的分配
    // 1. 数据以行为单位进行读取
    //    spark读取文件，采用的是hadoop的方式读取，所以一行一行读取，和字节数没有关系
    // 2. 数据读取时以偏移量为单位,偏移量不会被重复读取
    // 16byte / 2 = 8byte
    // 16 / 8 = 2(分区)

    /*
    1234567ij@@
    89@@
    0

    [0, 8]   => 1234567ij  （一行都读）
    [8, 14]  => 890

     */

    // 【1,2】，【3】，【】
    rdd.saveAsTextFile("output")

    // TODO 关闭
    sc.stop()

  }


}
