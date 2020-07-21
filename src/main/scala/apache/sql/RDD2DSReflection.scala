package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object RDD2DSReflection {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val dataSet: Dataset[String] = sparkSession.read.textFile("src\\main\\resources\\people.txt")
    val dataRdd: RDD[String] = sparkSession.sparkContext.textFile("src\\main\\resources\\people.txt")
    dataRdd.foreach(println)
//    Justin, 19
//    Michael, 29
//    Andy, 30
    dataSet.show()
    // 自动转成dataset,类型不对，是string应该
//    +-----------+
//    |      value|
//    +-----------+
//    |Michael, 29|
//    |   Andy, 30|
//    | Justin, 19|
//    +-----------+

    //将这个RDD转成dateset
    val dataRddPerson: RDD[Person] = dataRdd.map(_.split(","))
          .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    dataRddPerson.foreach(println)
//    Person(Justin,19)
//    Person(Michael,29)
//    Person(Andy,30)
    import sparkSession.implicits._
    val dataSetPerson: Dataset[Person] = dataRddPerson.toDS()
    dataSetPerson.show()
//    +-------+---+
//    |   name|age|
//    +-------+---+
//    |Michael| 29|
//    |   Andy| 30|
//    | Justin| 19|
//    +-------+---+

    dataSetPerson.createOrReplaceTempView("people")
    val teenagersDF: DataFrame = sparkSession.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    teenagersDF.show()
//    +------+---+
//    |  name|age|
//    +------+---+
//    |Justin| 19|
//    +------+---+

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
//    +------------+
//    |       value|
//    +------------+
//    |Name: Justin|
//    +------------+
    teenagersDF.map(teenager => "Name: " + teenager(0) + "  age: "+ teenager(1)).show()
//    +--------------------+
//    |               value|
//    +--------------------+
//    |Name: Justin  Age...|
//    +--------------------+
    //为什么age变成...

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
//    +------------+
//    |       value|
//    +------------+
//    |Name: Justin|
//    +------------+
    teenagersDF.map(teenagers => teenagers.getAs[Long]("age")).show()
//    +-----+
//    |value|
//    +-----+
//    |   19|
//    +-----+

    // 不太理解
    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
  }
}
