package stu.cfl.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object Spark04_Acc_WordCount {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        // 累加器 : WordCount
        // 创建累加器对象
        val rdd = sc.makeRDD(List("hello", "spark", "hello"))
        val wcAcc = new MyAccumulator
        // 向Spark进行注册
        sc.register(wcAcc, "wc")

        rdd.foreach(
            word => wcAcc.add(word)
        )
        println(wcAcc)

        sc.stop()

    }

    /*
      自定义数据累加器：WordCount

      1. 继承AccumulatorV2, 定义泛型
         IN : 累加器输入的数据类型 String
         OUT : 累加器返回的数据类型 mutable.Map[String, Long]

      2. 重写方法（6）
     */
    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{
        private var wcMap = mutable.Map[String, Long]()

        // 判断是否是初始状态
        override def isZero: Boolean = {
            wcMap.isEmpty
        }

        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            new MyAccumulator
        }

        override def reset(): Unit = {
            wcMap.clear()
        }

        // 实现累计逻辑
        override def add(v: String): Unit = {
            val l = wcMap.getOrElse(v, 0L) + 1
            wcMap.update(v, l)
        }

        // Driver合并多个累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val map2 = other.value
            map2.foreach{
                case (word, count) => {
                    val newCount = wcMap.getOrElse(word, 0L) + count
                    wcMap.update(word, newCount)
                }
            }
        }

        // 累加器结果
        override def value: mutable.Map[String, Long] = {
            wcMap
        }
    }
}
