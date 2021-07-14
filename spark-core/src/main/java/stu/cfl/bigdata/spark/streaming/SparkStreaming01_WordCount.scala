package stu.cfl.bigdata.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("localhost[*]").setAppName("streamingWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // TODO 逻辑处理
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordTuple: DStream[(String, Int)] = words.map((_, 1))
    val wordCount = wordTuple.reduceByKey(_ + _)
    wordCount.print()  // 打印信息


    // TODO 关闭环境
    // 由于sparkstreaming采集器是长期执行的任务，所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束，所以不能让main结束
    // 启动采集器
    ssc.start()
    // 等待采集器关闭
    ssc.awaitTermination()
//    ssc.stop()
  }
}
