package apache.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SQLExample").setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val dataFrame = sparkSession.read.json("src\\main\\resources\\people.json")
    // Register the DataFrame as a SQL temporary view
    dataFrame.createOrReplaceTempView("people")
    val frame = sparkSession.sql("select * from people")
    frame.show()


    //新创建一个session，报错，找不到 Table or view not found: people;
    //sparkSession.newSession().sql("select * from people").show()
    //20/07/21 11:20:04 INFO SparkSqlParser: Parsing command: select * from people
    //Exception in thread "main" org.apache.spark.sql.AnalysisException: Table or view not found: people; line 1 pos 14

    //注册成全局视图，只有spark程序关闭时，才会失效
    //Global temporary view is tied to a system preserved database global_temp,
    // and we must use the qualified name to refer it,
    // e.g. SELECT * FROM global_temp.view1.
    //类似放在global_temp库下了，访问时候把库名加上，不然找不到表
    dataFrame.createGlobalTempView("glo_people")
    sparkSession.sql("select * from global_temp.glo_people").show()
    sparkSession.newSession().sql("select * from global_temp.glo_people").show()
  }
}
