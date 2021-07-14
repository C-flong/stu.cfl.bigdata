package stu.cfl.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.nio.file.attribute.UserDefinedFileAttributeView

object Spark03_SparkSql_UDAF {
  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val df: DataFrame = spark.read.json("datas/user.json")
    df.createTempView("user")
//    spark.sql("select concat('name:', username), age from user").show()
    spark.udf.register("MyAvgUDAF", new MyAvgUDAF)

    spark.sql("select MyAvgUDAF(age) from user").show()


    // TODO 关闭环境
    spark.close()
  }
  /*
  自定义聚合函数类：计算年龄的平均值
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction{

    // 输入数据结构
    override def inputSchema: StructType = {
      StructType(  // 由于StructType是样例类，case class StructType，所以创建对象可以直接通过apply进行创建对象
        Array(StructField("age", LongType))  // Array自带有伴生对象，故也可省略new来构建对象
      )
    }

    // 缓冲区结构
    override def bufferSchema: StructType = {
      StructType(  // 由于StructType是样例类，case class StructType，所以创建对象可以直接通过apply进行创建对象
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )  // Array自带有伴生对象，故也可省略new来构建对象
      )
    }

    // 输出数据类型
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲去初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0L)  // StructField("total", LongType)
      buffer.update(1, 0L)  // StructField("count", LongType)

//      buffer(0) = 0L
//      buffer(1) = 0L
    }

    // 根据输入的值更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0)+input.getLong(0))
      buffer(1) = buffer.getLong(1) + 1
    }

    // 缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0)+buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1)+buffer2.getLong(1))
    }

    // 计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0)/buffer.getLong(1)
    }
  }
}
