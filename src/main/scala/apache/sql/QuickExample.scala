package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object QuickExample {
  def main(args: Array[String]): Unit = {
//    val sparkSession = SparkSession
//                          .builder()
//                          .appName("QuickExample")
//                          .config("spark.some.config.option", "some-value")
//                          .getOrCreate()
    //报错 org.apache.spark.SparkException: A master URL must be set in your configuration


    val sparkConf = new SparkConf().setAppName("QuickExample").setMaster("local[*]")
    val sparkSession = SparkSession
                          .builder()
                          .appName("QuickExample")
                          .config(sparkConf)
                          .getOrCreate()
    import sparkSession.implicits._
    val dataFrame = sparkSession.read.json("src\\main\\resources\\people.json")

    dataFrame.show()
//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//      |  30|   Andy|
//      |  19| Justin|
//      +----+-------+

    dataFrame.printSchema()
//    root
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)

    dataFrame.select("name").show()
//    +-------+
//    |   name|
//    +-------+
//    |Michael|
//    |   Andy|
//    | Justin|
//    +-------+

    //dataFrame.select("name", "age"+1).show() 会报错

    dataFrame.select($"name",$"age"+1).show()
//    +-------+---------+
//    |   name|(age + 1)|
//      +-------+---------+
//    |Michael|     null|
//      |   Andy|       31|
//      | Justin|       20|
//      +-------+---------+

    dataFrame.filter($"age" > 21).show()
//    +---+----+
//    |age|name|
//    +---+----+
//    | 30|Andy|
//    +---+----+

    dataFrame.groupBy("age").count().show()
//    +----+-----+
//    | age|count|
//    +----+-----+
//    |  19|    1|
//      |null|    1|
//      |  30|    1|
//      +----+-----+
  }
}
