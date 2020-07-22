package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object WC3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val word: Dataset[String] = sparkSession.read.textFile("src\\main\\resources\\people.txt")
    val path = "src\\main\\resources\\people.json"
    val peopleDS: DataFrame = sparkSession.read.json(path)
    val wordDataFrame: DataFrame = sparkSession.read.text("src\\main\\resources\\people.txt")
    val wordDataSet: Dataset[WordCount] = wordDataFrame.as[WordCount]
    wordDataSet.show()
    peopleDS.show()
    word.show()
    
  }
}
