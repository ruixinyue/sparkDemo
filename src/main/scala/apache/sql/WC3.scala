package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object WC3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val path = "src\\main\\resources\\people.json"
    val peopleDF: DataFrame = sparkSession.read.json(path)
    val personDS: Dataset[Person] = peopleDF.as[Person]
    peopleDF.createOrReplaceTempView("peopleDF")
    personDS.createOrReplaceTempView("personDS")
//    val wordDataFrame: DataFrame = sparkSession.read.text("src\\main\\resources\\people.txt")
//    val wordDataSet: Dataset[WordCount] = wordDataFrame.as[WordCount]
//    wordDataSet.show()
//    sparkSession.sql("select * from peopleDF").show()
//    sparkSession.sql("select * from personDS").show()
//    peopleDF.show()
//    word.show()

val word: Dataset[String] = sparkSession.read.textFile("src\\main\\resources\\people.txt")
    val dataSet: Dataset[String] = word.flatMap(_.split(","))
    dataSet.show()
//    +-------+
//    |  value|
//    +-------+
//    |Michael|
//    |     29|
//      |   Andy|
//    |     30|
//      | Justin|
//    |     29|
//      +-------+
  }
}
