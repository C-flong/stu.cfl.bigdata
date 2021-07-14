package stu.cfl.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

object Spark03_SparkSql_UDAF1 {
  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val df: DataFrame = spark.read.json("datas/user.json")
    df.createTempView("user")
//    spark.sql("select concat('name:', username), age from user").show()
    spark.udf.register("MyAvgUDAF", functions.udaf(new MyAvgUDAF))

    spark.sql("select MyAvgUDAF(age) from user").show()


    // TODO 关闭环境
    spark.close()
  }
  /*
  自定义聚合函数类：计算年龄的平均值
  继承：import org.apache.spark.sql.expressions.Aggregator
  IN：输入数据类型
  BUF：
  OUT：输出数据类型
   */
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long]{

    // 初始值
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入值更新缓冲区的数据
    override def reduce(b: Buff, a: Long): Buff = {
      b.total += a
      b.count += 1
      b
    }

    // 合并
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total += b2.total
      b1.count += b2.count
      b1
    }

    // 计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
