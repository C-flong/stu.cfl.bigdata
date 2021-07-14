package stu.cfl.bigdata.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamingWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // TODO 逻辑处理

    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
    messageDS.print()

    // TODO 关闭环境
    // 由于sparkstreaming采集器是长期执行的任务，所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束，所以不能让main结束
    // 启动采集器
    ssc.start()
    // 等待采集器关闭
    ssc.awaitTermination()
//    ssc.stop()
  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flag = true
    override def onStart(): Unit = {
      // 需要另起一个线程用于采集数据（这边自己生成数据，模拟采集的情况）
      new Thread(new Runnable {
        override def run(): Unit = {
          while(flag){
            val message = "采集的数据为：" + new Random().nextInt(10).toString
            // 采集数据后需要进行封装（由SparkStreaming内部完成）
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()

    }

    override def onStop(): Unit = {
      // 线程停止后执行
      flag = false

    }
  }
}
