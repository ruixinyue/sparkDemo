package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object WC2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val dataRdd: RDD[String] = sparkSession.sparkContext.textFile("src\\main\\resources\\people.txt")
    import sparkSession.implicits._
    val dataRow: RDD[Row] = dataRdd.flatMap(_.split(",")).map(attributes => Row(attributes.trim))
    val word: StructField = StructField("word", StringType, nullable = true)
    val wordStructType = StructType(Seq(word))
    // 手动创建DataFrame，指定列名叫word
    val wordDataFrame = sparkSession.createDataFrame(dataRow, wordStructType)
    wordDataFrame.createTempView("wordCount")
    val res = sparkSession.sql("select word, count(1) from wordCount group by word")
    res.show()
//    +-------+--------+
//    |   word|count(1)|
//    +-------+--------+
//    |     29|       2|
//    |     30|       1|
//    |Michael|       1|
//    |   Andy|       1|
//    | Justin|       1|
//    +-------+--------+
  }
}
