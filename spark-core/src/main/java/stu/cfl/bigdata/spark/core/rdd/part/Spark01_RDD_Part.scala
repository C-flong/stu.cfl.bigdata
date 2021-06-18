package stu.cfl.bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxxxx"),
      ("cba", "xxxxxxxxx"),
      ("wnba", "xxxxxxxxx"),
      ("nba", "xxxxxxxxx"),
    ),3)

    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner())
    partRDD.saveAsTextFile("output")

    sc.stop()
  }
  /**
   * 自定义分区器
   */
  class MyPartitioner extends Partitioner{
    // 分区数量
    override def numPartitions: Int = 3

    // 按照key值进行分区，返回一个int类型值表示分区号，从0开始
    override def getPartition(key: Any): Int = {
      key match{
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }
}
