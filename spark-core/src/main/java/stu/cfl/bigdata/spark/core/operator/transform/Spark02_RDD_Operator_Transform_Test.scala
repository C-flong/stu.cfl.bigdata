package stu.cfl.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 mapPartitions
    val rdd = sc.makeRDD(List[Int](1, 2, 3, 4), 2)
    val mapPRDD = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )  // 传入为迭代器，返回一个迭代器

    // mapPartitions : 可以以分区为单位进行数据转换操作
    //                 但是会将整个分区的数据加载到内存进行引用
    //                 如果处理完的数据是不会被释放掉，存在对象的引用。
    //                 在内存较小，数据量较大的场合下，容易出现内存溢出。

    mapPRDD.collect().foreach(println)

    sc.stop()
  }
}
