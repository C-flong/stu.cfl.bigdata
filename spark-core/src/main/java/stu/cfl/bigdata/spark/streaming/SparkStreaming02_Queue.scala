package stu.cfl.bigdata.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SparkStreaming02_Queue {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamingWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // TODO 逻辑处理
    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
    val mappedStream = inputStream.map((_, 1))
    val reduceStream = mappedStream.reduceByKey(_ + _)
    reduceStream.print()


    // TODO 关闭环境
    // 由于sparkstreaming采集器是长期执行的任务，所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束，所以不能让main结束
    // 启动采集器
    ssc.start()

    for(i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    // 等待采集器关闭
    ssc.awaitTermination()
//    ssc.stop()
  }
}
