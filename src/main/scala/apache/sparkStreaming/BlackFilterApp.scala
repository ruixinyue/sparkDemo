package apache.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._


object BlackFilterApp {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkconf,Seconds(5))

    /**
     * 构建黑名单（要过滤的数据）
     */
    val blacks = List("zs", "ls")  // 一般这条在数据库中，用 read 读进来即可
    val blacksRDD = ssc.sparkContext.parallelize(blacks)//转成RDD
      .map(x => (x, true))
    //将这个元素 x 重新定位为一个新字段  （x，true）
    //(("zs","true"),("ls","true"))

    val lines = ssc.socketTextStream("localhost", 8888)

    //20180808,zs 原来的格式
    //zs,20180808,zs 处理后的格式
    //取index=1的元素，然后在跟上它自身
    val clicklogs = lines.map(x => (x.split(",")(1), x))
      .transform(rdd => {
        //blacksRDD进行map操作后它是RDD格式，此处的lines进行map操作后，它是DStream[U]格式，
        //所以此处，要将DStream和RDD进行联合，就要使用transform算子，
        //通过将RDD-to-RDD函数应用于源DStream的每个RDD来返回新的DStream。
        //这可以用于在DStream上执行任意RDD操作。
        rdd.leftOuterJoin(blacksRDD)
          //进行表的左外连接 leftOuterJoin
          //
          // 端口传进来的数据，经过处理后
          // zs,20180808,zs
          // ls,20180808,ls
          // ww,20180808,ww
          //
          // 黑名单中的数据
          // (("zs","true"),("ls","true"))
          //
          //进行关联后的数据
          // (zs:[<20180808,zs>,<true>])  x
          // (ls:[<20180808,ls>,<true>])  x
          // (ww:[<20180808,ww>,<false>])  ==> tuple 1
          .filter(x => x._2._2.getOrElse(false) != true)
          // 过滤(zs: [<20180808,zs> ,<true>])中，第二个元素的中的第二个元素，判断是否等于true，如果不为true，则返回false，
          // 此处运行后，就只剩下为false的元素了 (ww:[<20180808,ww>,<false>]) ，只有这一条了
          .map(x => x._2._1)
        //取(zs,[<20180808,zs>,<true>])中第二个元素的第一个元素  （tuple的使用）
      })

    clicklogs.print()  //在控制台打印信息   (这块结果应该是有问题的,它只显示最后一个), 应该在 filter或map这块,它是有问题的, 感觉问题在 map, 对元组的掌握,还有待提高

    ssc.start()
    ssc.awaitTermination()
  }
}