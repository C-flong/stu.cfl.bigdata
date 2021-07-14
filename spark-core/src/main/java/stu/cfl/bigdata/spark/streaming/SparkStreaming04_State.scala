package stu.cfl.bigdata.spark.streaming


import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}



object SparkStreaming04_State {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamingWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // 使用缓冲区中的数据，需要定义检查点路径
    ssc.checkpoint("cp")

    // TODO 逻辑处理

    val datas = ssc.socketTextStream("localhost", 9999)
    val wordTuple = datas.map((_, 1))

    // updateStateByKey：根据key对数据的状态进行更新
    // 参数1：seq 表示相同key的value组合
    // 参数2：buff 表示缓冲区中相同key的value数据

    val wordCount = wordTuple.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )

    wordCount.print()

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
