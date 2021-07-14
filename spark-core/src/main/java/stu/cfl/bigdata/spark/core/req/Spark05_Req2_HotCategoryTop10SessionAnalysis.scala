package stu.cfl.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_HotCategoryTop10SessionAnalysis {
  def main(args: Array[String]): Unit = {
    // TODO: Top10热门种类
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    // 1、获取数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val top10Category:Array[String] = Top10Category(actionRDD)

    val filterActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 判断是否在top10里
          top10Category.contains(datas(6))
        }
        else {
          false
        }
      }
    )

    val reduceRDD = filterActionRDD.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1) // （（点击，session），1）
      }
    ).reduceByKey(_ + _)

    val mapRDD = reduceRDD.map {
      case ((t1, t2), t3) => {
        (t1, (t2, t3))
      }
    }

    val groupRDD = mapRDD.groupByKey()

    val resultRDD:RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        // 按照点击次数进行排序
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )

    resultRDD.collect().foreach(println)


    sc.stop()
  }

  def Top10Category(actionRDD: RDD[String]): Array[String] = {
    // 2、统计品类的点击数量
    val sourceRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        }
        else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(
            id => {
              (id, (0, 1, 0))
            }
          )
        }
        else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(
            id => {
              (id, (0, 0, 1))
            })
        }
        else {
          Nil
        }
      }
    )


    // 5、将品类进行排序，并且取前10
    // 依次按照2，3，4进行排序
    // 可以利用元组排序的规律实现
    // (品类, (点击数量， 下单数量， 支付数量))
    // 选择使用cogroup（分组+）进行连接


    val analysisRDD = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

//    analysisRDD.map(_._1)
    analysisRDD.sortBy(_._2, false).take(10).map(_._1)

  }
}
