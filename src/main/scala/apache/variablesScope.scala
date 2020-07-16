package apache

import org.apache.spark.{SparkConf, SparkContext}

object variablesScope {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("variablesScope")
    val sparkContext = new SparkContext(sparkConf)
    val array = Array(1, 2, 3, 4, 5)
    val myRdd = sparkContext.parallelize(array)
    var sum = 0
    val eachRdd = myRdd.foreach(println)
    val newRdd = myRdd.foreach(x => sum += x)
    println(sum)
  }
}
