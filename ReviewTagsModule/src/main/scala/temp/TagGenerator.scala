package temp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fengqijie
  * 2019/2/28 19:42
  */
object TagGenerator {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("TagGenerator").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val poi_tags: RDD[String] = sc.textFile("hdfs://hadoop0:8020/input/temptags.txt")

    val poi_taglist: RDD[(String, String)] = poi_tags.map(e => e.split("\t"))
      .filter(e => e.length == 2)
      // 772897793 -> "回头客", "上菜快", "环境优雅", "性价比高", "菜品不错"
      .map(e => e(0) -> ReviewTags.extractTags(e(1)))
      // 过滤评论串不为0
      .filter(e => e._2.length > 0)
      // 772897793 -> ["回头客", "上菜快", "环境优雅", "性价比高", "菜品不错"]
      .map(e => e._1 -> e._2.split(","))
      // 772897793 -> "回头客", 772897793 -> "上菜快", 772897793 -> "环境优雅",
      // 772897793 -> "性价比高", 772897793 -> "菜品不错"
      .flatMapValues(e => e)
      // (772897793,"回头客") -> 1, (772897793,"上菜快") -> 1, (772897793,"环境优雅") -> 1
      .map(e => (e._1, e._2) -> 1)
      // 聚合 (772897793,"回头客") -> 340, (772897793,"上菜快") -> 560
      .reduceByKey(_ + _)
      // 772897793 -> List("回头客",340), 772897793 -> List("上菜快",560)
      .map(e => e._1._1 -> List((e._1._2, e._2))) // 元组不能聚合，列表可以聚合
      // 772897793 -> List(("回头客",340), ("上菜快",560), ...)    把key相同的都放在一个list里面
      .reduceByKey(_ ::: _) // ::: 表示列表累加
      .map(e => e._1 -> e._2.sortBy(_._2)
      // 772897793 -> List(("回头客", 780), ("上菜快", 560), ("环境优雅", 340), ...)
      .reverse.take(10)
      // 772897793 -> List("回头客":780,"上菜快":560,"环境优雅":340), ...)
      .map(a => a._1 + ":" + a._2.toString)
      // 772897793 -> "回头客":780,"上菜快":560,"环境优雅":340, ...
      .mkString(","))

    val result: RDD[String] = poi_taglist.map(e => e._1 + "\t" + e._2)
    result.saveAsTextFile("hdfs://hadoop0:8020/out/temptags_out")

    sc.stop()

  }

}
