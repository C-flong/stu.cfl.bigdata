package stu.cfl.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Req_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    // TODO: Top10热门种类
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    // 1、获取数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
//    actionRDD.cache()

    // (品类ID, 点击数量) => (品类ID, (点击数量, 0, 0))
    // (品类ID, 下单数量) => (品类ID, (0, 下单数量, 0))
    //                    => (品类ID, (点击数量, 下单数量, 0))
    // (品类ID, 支付数量) => (品类ID, (0, 0, 支付数量))
    //                    => (品类ID, (点击数量, 下单数量, 支付数量))
    // ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )

    // 5. 将品类进行排序，并且取前10名
    //    点击数量排序，下单数量排序，支付数量排序
    //    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    //    ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
    val acc = new HotCategoryAccumulator
    sc.register(acc, "hotCategory")

    // 2、统计品类的点击数量
    actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          acc.add(datas(6), "click")
        }
        else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(
            id => {
              acc.add(id, "order")
            }
          )
        }
        else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(
            id => {
              acc.add(id, "pay")
            })
        }
      }
    )
    val accValue: mutable.Map[String, HotCategory] = acc.value
    val categories = accValue.map(_._2)

    // 5、将品类进行排序，并且取前10
    // 依次按照2，3，4进行排序
    // 可以利用元组排序的规律实现
    // (品类, (点击数量， 下单数量， 支付数量))
    // 选择使用cogroup（分组+）进行连接

    val sort = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    )

    sort.take(10).foreach(println)

    sc.stop()
  }
  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt : Int, var payCnt : Int ){

  }
  /**
   * IN: (品类ID, 行为类型)
   * OUT: mutable.Map[String, HotCategory]
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]{
    val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {

      val cid = v._1
      val actionType = v._2
      val category = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))

      if(actionType == "click"){
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }

      hcMap.update(cid, category)

    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map = other.value
      map.foreach{
        case (cid, hc) => {
          val category = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = {
      hcMap
    }
  }
}
