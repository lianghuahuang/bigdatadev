import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 * ${DESCRIPTION}
 *
 * @author lianghuahuang
 * @date 2021/8/23
 *
 * */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]").appName("FileWordCount")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext;
    val words = sc.wholeTextFiles("file:///e:/sparkTestFile/")
    words.flatMap {
      case (path, text) =>
        text.split(" ").map(word => (word, path))
    }.map {
      case (word, path) => ((word, path), 1)
    }.reduceByKey (_+_).map {
      case ((word, path), n) => (word, (path.substring(path.lastIndexOf("/") + 1), n))
    }.groupByKey.mapValues(iterator => iterator.mkString(", ")).foreach(println)
    sc.stop()
  }
}
