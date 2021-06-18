package stu.cfl.bigdata.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test01 {
  def main(args: Array[String]): Unit = {
    // TODO: Top10热门种类
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    // 1、获取数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    // 3、统计品类下单的数量
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )
    //    orderid => 1,2,3
    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cids = datas(8).split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 4、统计品类支付的数量
    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    // orderid => 1,2,3
    // 【(1,1)，(2,1)，(3,1)】
    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id=>(id, 1))
      }
    ).reduceByKey(_+_)

    // 5、将品类进行排序，并且取前10
    // 依次按照2，3，4进行排序
    // 可以利用元组排序的规律实现
    // (品类, (点击数量， 下单数量， 支付数量))


    sc.stop()
  }
}
