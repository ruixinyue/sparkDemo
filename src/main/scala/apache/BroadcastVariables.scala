package apache

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariables {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("broadcast variables").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val broadcast = sparkContext.broadcast(Array(1, 2, 3))
    broadcast.value.foreach(println)
  }

}
