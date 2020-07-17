package apache

import org.apache.spark.{SparkConf, SparkContext}

object Accumulators {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Accumulators").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val accumulator = sparkContext.longAccumulator("my accumulator")
    val myRdd = sparkContext.parallelize(Array(1, 2, 3))
    val sum = myRdd.foreach(x => accumulator.add(x))
    println(accumulator.value)
  }
}
