package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataSources {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val jsonDataFrame = sparkSession.read.json("src\\main\\resources\\people.json")
    val jsonDateFrame2 = sparkSession.read.format("json").load("src\\main\\resources\\people.json")
    //两种方式读取都可以
    jsonDataFrame.show()
    jsonDateFrame2.show()

    //直接读取文件并查询
    val dataFrame = sparkSession.sql("select * from json.`src\\main\\resources\\people.json`")
    dataFrame.show()
//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//    |  30|   Andy|
//    |  19| Justin|
//    +----+-------+
  }
}
