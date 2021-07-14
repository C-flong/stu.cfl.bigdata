package stu.cfl.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageflowAnalysis {
  def main(args: Array[String]): Unit = {
    // TODO: Top10热门种类
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    // 1、获取数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionDataRDD.cache()

    // TODO 对指定页面进行统计
    val ids: List[Int] = List(1, 2, 3, 4, 5, 6, 7)
    val okflowIds: List[(Int, Int)] = ids.zip(ids.tail)

    val pageIdToCountMap: Map[Long, Int] = actionDataRDD.filter(
      action => {
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1)
      }
    ).reduceByKey(_ + _).collect().toMap

    // TODO 统计跳转页面次数
    // 统计一次会话中连续的页面变化
    val sessionRDD = actionDataRDD.groupBy(_.session_id)

    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

        // （1，2，3，4）=> （（1，2）， （2，3）， （3，4））
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        pageflowIds.filter(
          action => {
            okflowIds.contains(action)
          }
        ).map(
          t => {
            (t, 1)
          }
        )
      }
    )
    val flatMapRDD = mvRDD.flatMap(_._2)
    val reduceRDD: RDD[((Long, Long), Int)] = flatMapRDD.reduceByKey(_ + _)

    reduceRDD.foreach(
      data => {
        val lon: Int = pageIdToCountMap.getOrElse(data._1._1, 0)
        println(s"页面${data._1._1}跳转到页面${data._1._2}单跳转换率为:" + ( data._2.toDouble/lon ))
      }

    )

    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Long,// 用 户 的 ID
    session_id: String,//Session 的 ID
    page_id: Long,// 某 个 页 面 的 ID
    action_time: String,//动作的时间点
    search_keyword: String,//用户搜索的关键词
    click_category_id: Long,// 某 一 个 商 品 品 类 的 ID
    click_product_id: Long,// 某 一 个 商 品 的 ID
    order_category_ids: String,//一次订单中所有品类的 ID 集合
    order_product_ids: String,//一次订单中所有商品的 ID 集合
    pay_category_ids: String,//一次支付中所有品类的 ID 集合
    pay_product_ids: String,//一次支付中所有商品的 ID 集合
    city_id: Long
  )//城市 id

}
