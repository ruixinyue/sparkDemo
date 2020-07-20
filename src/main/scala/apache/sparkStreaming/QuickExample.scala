package apache.sparkStreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object QuickExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
//    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(1))
    val line = streamingContext.socketTextStream("localhost", 9999)
    val words = line.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    //开始执行，等待执行结束
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
