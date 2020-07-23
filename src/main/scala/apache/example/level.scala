package apache.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 需求：计算玩家等级
 * point表，point_emp表中level字段都存储玩家等级，
 * 直取point表中的玩家等级，
 * 码值对应关系：1-青铜，2-黄金，3-铂金，4-钻石，5-王者,
 * 如果用户只存在在point_emp表中，这种玩家属于游客，对应等级为游客.
 * 玩家范围：point_emp
 */
object level {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("leval").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val point: DataFrame = sparkSession.read.json("src\\main\\resources\\point.json")
    val point_emp: DataFrame = sparkSession.read.json("src\\main\\resources\\point_emp.json")
    point.createOrReplaceTempView("point")
    point_emp.createOrReplaceTempView("point_emp")
    val res = sparkSession.sql(
      """
        |select id,
        |case when level ="0" then "游客"
        |      when level ="1" then "青铜"
        |      when level ="2" then "黄金"
        |      when level ="3" then "铂金"
        |      when level ="4" then "钻石"
        |      when level ="5" then "王者" end as level
        |from (select pe.id as id,
        |case when pe.level ="" then "0" else p.level end as level
        |from point_emp pe left join point p
        |on p.id = pe.id) t
        |""".stripMargin)
    res.show()
//    +----+-----+
//    |  id|level|
//    +----+-----+
//    |0001|   青铜|
//    |0002|   铂金|
//    |0003|   黄金|
//    |0004|   钻石|
//    |0005|   王者|
//    |0006|   游客|
//    +----+-----+
    //如何用rdd方式实现
//    val frame = point_emp.join(point)
//    frame.show()
  }
}
