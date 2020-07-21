package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //用case class创建DataSet
    import sparkSession.implicits._
    // toDS()需要引入隐式转换
    val myds = Seq(Person("jan", 12)).toDS()
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    myds.show()
    caseClassDS.show()

    //普通DS
    val primitiveDS = Seq(Array(1, 2, 3)).toDS()
//    +---------+
//    |    value|
//    +---------+
//    |[1, 2, 3]|
//    +---------+

    val commentDS = Seq(1, 2, 3).toDS()
//    +-----+
//    |value|
//    +-----+
//    |    1|
//    |    2|
//    |    3|
//    +-----+
    primitiveDS.show()
    commentDS.show()
    val array = commentDS.map(_ + 1).collect()
    array.foreach(println)
//    2
//    3
//    4

    //DataFrame.as[CaseClass]
    //case class Person(name: String, age: Int)
    // 报错信息
    //Exception in thread "main" org.apache.spark.sql.AnalysisException:
    // Cannot up cast `age` from bigint to int as it may truncate
    //The type path of the target object is:
    //- field (class: "scala.Int", name: "age")
    //- root class: "apache.sql.Person"
    //You can either add an explicit cast to the input data
    // or choose a higher precision type of the field in the target object;

    //case class Person(name: String, age: Long) 改成Long就不报错了，为什么
    val peopleDS = sparkSession.read.json("src\\main\\resources\\people.json").as[Person]
    peopleDS.show()
  }
}


// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface
case class Person(name: String, age: Long)
