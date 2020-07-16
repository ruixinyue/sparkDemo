package apache

import org.apache.spark.{SparkConf, SparkContext}

object quick_start {
  def main(args: Array[String]): Unit = {
    // 创建saprkcontext
    val conf = new SparkConf().setAppName("quickStart").setMaster("local")
    val sc = new SparkContext(conf)
    // 读取文件
    val logData = sc.textFile("D:\\Program Files\\JetBrains\\IdeaProjects\\sparkDemo\\src\\main\\resources\\testfile",
      2).cache()
    // 计算包含a，b的行数
    val aNum = logData.filter(_.contains("a")).count()
    val bNum = logData.filter(_.contains("b")).count()
    println(s"a=$aNum b=$bNum")
    val sum = logData.count()
    val firstLine = logData.first()
    println("文件总数：" + sum)
    println("第一行文件： "+ firstLine)
    // 包含最多单词的行
    val singleworld = logData.map(_.split(" ").size)
    // singleworld.foreach(println)
    // 为什么可以写成元组格式
    val bigline = singleworld.reduce((a, b) => if (a > b) a else b)
    // worldcount
    val wc = logData.flatMap(line => line.split(" "))
                    .map(word => (word,1))
                    .reduceByKey((a,b)=> a+b)
    println(wc)
    wc.foreach(println)
    println("============")
    // 什么意思
    wc.collect()
    println(singleworld)
    println(bigline)
    sc.stop()
  }
}
