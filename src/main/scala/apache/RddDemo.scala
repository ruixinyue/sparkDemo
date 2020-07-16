package apache

import org.apache.spark.{SparkConf, SparkContext}

object RddDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("demo").setMaster("local")
    val sc = new SparkContext(conf)
    val myRdd = sc.parallelize(Array(1, 2, 3, 4))
    myRdd.foreach(println)
  }
}
