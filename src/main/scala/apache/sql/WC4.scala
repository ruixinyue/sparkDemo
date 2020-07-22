package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object WC4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val dataRdd: RDD[String] = sparkSession.sparkContext.textFile("src\\main\\resources\\people.txt")
    val dataFrame = dataRdd.flatMap(_.split(",")).toDF()
    dataFrame.createOrReplaceTempView("people")
    // 未指定schema，默认的列名是value
    val res = sparkSession.sql("select value,count(1) from people group by value")
    res.show()
//    +-------+--------+
//    |  value|count(1)|
//    +-------+--------+
//    |     29|       2|
//    |Michael|       1|
//    |   Andy|       1|
//    |     30|       1|
//    | Justin|       1|
//    +-------+--------+
  }
}
