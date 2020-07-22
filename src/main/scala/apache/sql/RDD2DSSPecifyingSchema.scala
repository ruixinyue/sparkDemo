package apache.sql

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, _}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * 将一个String类型的
 */
object RDD2DSSPecifyingSchema {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val dataRdd: RDD[String] = sparkSession.sparkContext.textFile("src\\main\\resources\\people.txt")

    //创建schema ，StructType(StructField(fieldName,StringType,nullable = true))
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, org.apache.spark.sql.types.StringType, nullable = true))
    val schema = StructType(fields)

    import sparkSession.implicits._
    //创建符合schema格式的rdd
    val rowRdd = dataRdd.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
    val dataFrame = sparkSession.createDataFrame(rowRdd, schema)
    dataFrame.show()
//    +-------+---+
//    |   name|age|
//    +-------+---+
//    |Michael| 29|
//    |   Andy| 30|
//    | Justin| 19|
//    +-------+---+
    dataFrame.createOrReplaceTempView("people")
    val results = sparkSession.sql("select * from people")
    results.map(attributes => "Name: "+attributes(0)).show()
//    +-------------+
//    |        value|
//    +-------------+
//    |Name: Michael|
//    |   Name: Andy|
//    | Name: Justin|
//    +-------------+

  }
}
