package apache.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Spark实现分组取topN案例
 * 描述：有订单数据order.txt文件，文件字段的分割符号","，
 * 其中字段依次表示订单id，商品id，交易额。样本数据如下：
 * Order_00001,Pdt_01,222.8
 * Order_00001,Pdt_05,25.8
 * Order_00002,Pdt_03,522.8
 * Order_00002,Pdt_04,122.4
 * Order_00002,Pdt_05,722.4
 * Order_00003,Pdt_01,222.8
 *
 * 需求：问题：使用sparkcore，求每个订单中成交额最大的商品id
 */
object TopN {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("orderTopN").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val orderRDD = sparkContext.textFile("src\\main\\resources\\order.txt")
    orderRDD.foreach(println(_))
    val groupRDD: RDD[(String, Iterable[mutable.HashMap[String, Double]])] = orderRDD.map(_.split(","))
      .map(item => (item(0), mutable.HashMap(item(1) -> item(2).toDouble)))
      .groupByKey()
    groupRDD.foreach(println)
//    (Order_00003,CompactBuffer(Map(Pdt_01 -> 222.8)))
//    (Order_00002,CompactBuffer(Map(Pdt_03 -> 522.8), Map(Pdt_04 -> 122.4), Map(Pdt_05 -> 722.4)))
//    (Order_00001,CompactBuffer(Map(Pdt_01 -> 222.8), Map(Pdt_05 -> 25.8)))

    groupRDD.map{
      case (orderId,iterable) =>

    }
//    groupRDD.map{
//      case (orderId,iterable) =>
//        var proId: String = null
//        val list = iterable.toList
//        val amount: List[Nothing] = List()
//        for (elem <- list) {
//          amount :+ elem._2.toLong
//        }
//        val maxAmount = amount.max
//        for (elem <- list) {
//          if(elem._2 == maxAmount){
//            proId = elem._1
//          }
//        }
//        println(orderId + "," + proId + "," + maxAmount )
//    }
  }
}
