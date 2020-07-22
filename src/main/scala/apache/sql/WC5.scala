package apache.sql


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * rdd操作实现wordcount
 * 1.切割，转元组，reduceByKey根据key进行统计
 */
object WC5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val dataRdd: RDD[String] = sparkContext.textFile("src\\main\\resources\\people.txt")
    val res = dataRdd.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    res.foreach(println)
//    ( 30,1)
//    (Michael,1)
//    (Andy,1)
//    ( 29,2)
//    (Justin,1)
  }
}
