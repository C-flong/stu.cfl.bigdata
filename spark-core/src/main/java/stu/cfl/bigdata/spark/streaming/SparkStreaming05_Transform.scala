package stu.cfl.bigdata.spark.streaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamingWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // 使用缓冲区中的数据，需要定义检查点路径
    ssc.checkpoint("cp")

    // TODO 逻辑处理

    val datas = ssc.socketTextStream("localhost", 9999)

    // Code: Driver端
    val transformDS: DStream[String] = datas.transform(
      rdd => {
        // Code: Driver端（周期性执行）
        rdd.map(
          data => {
            // Code: excutor端
            data
          }
        )
      }
    )

    // Code: drive端
    val mapDS: DStream[String] = datas.map(
      data => {
        // Code: excutor端
        data
      }
    )

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
