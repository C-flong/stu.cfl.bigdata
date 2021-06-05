package stu.cfl.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark 框架
    // TODO 建立和Spark框架的联系
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO 执行业务操作
    // 1、读取文件，获取一行一行数据
    // hello world
    val lines: RDD[String] = sc.textFile("datas")  // 读入文件，将文件中的数据按照行进行存储
//    lines.foreach(println)
    // 2、将文件中的每一行数据进行拆分
    // "hello world hello" => hello, world, hello
    val words: RDD[String] = lines.flatMap(_.split(" "))  // 扁平化处理, 将整体拆分成个体
    //    words.foreach(println)

    val wordToOne = words.map(
      word => (word, 1)
    )
    // 3、将数据进行分组
    // (hello, hello), (world)

//    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
//      word => word._1
//    )  // 根据第一个元素分组 结果为（hello, [(hello, 1), (hello, 1)]）

    // 4、对分组后的数据进行转换
    // (hello, 2), (world, 1)
//    val wordToCount = wordGroup.map{
//      case (word, list) => {
//        list.reduce(
//          (t1, t2) => {
//            (t1._1, t1._2+t2._2)
//          }
//        )
//      }
//    }

    //    wordToOne.reduceByKey( (x, y) => {x+y} )
    //    wordToOne.reduceByKey( (x, y) => x+y )  // 函数就一行可以把大括号省略
    val wordToCount = wordToOne.reduceByKey(_ + _)


    // 5、将转换结果进行打印
    wordToCount.foreach(println)


    // TODO 关闭连接
    sc.stop()
  }

}
