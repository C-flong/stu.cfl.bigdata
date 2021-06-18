package stu.cfl.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    // TODO: Top10热门种类
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    // 1、获取数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")


    // 2、统计品类的点击数量
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )
    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

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
    // 选择使用cogroup（分组+）进行连接
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        // Iterator是迭代器类，而Iterable是接口。
        val iter1 = clickIter.iterator
        var clickCnt = 0
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }

        val iter2 = clickIter.iterator
        var orderCnt = 0
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }

        val iter3 = clickIter.iterator
        var payCnt = 0
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    resultRDD.foreach(println)

    sc.stop()
  }
}
