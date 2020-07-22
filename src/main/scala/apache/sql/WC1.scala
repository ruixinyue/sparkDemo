package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WC1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val dataRdd: RDD[String] = sparkSession.sparkContext.textFile("src\\main\\resources\\people.txt")
    // 使用样例类，指定schema
    val dataFrame = dataRdd.flatMap(_.split(",")).map(attributes => WordCount(attributes)).toDF()
    dataFrame.show()
    dataFrame.createOrReplaceTempView("wordCount")
    val res = sparkSession.sql("select word, count(1) from wordCount group by word")
    res.show()
//    +-------+--------+
//    |   word|count(1)|
//    +-------+--------+
//    |     29|       2|
//    |Michael|       1|
//    |   Andy|       1|
//    |     30|       1|
//    | Justin|       1|
//    +-------+--------+
  }
}

case class WordCount(word: String)