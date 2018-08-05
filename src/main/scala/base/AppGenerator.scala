package base

import com.twitter.scalding.Args
import model.App
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.PathUtils._

object AppGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args      = Args(mainArgs)
    val day       = semanticDate(args.getOrElse("day", "-1"))
    val inputPath = RawDatePath(app_data_path, day.toInt)

    val conf = new SparkConf()
      .setAppName("App Data Job")
      .set("spark.sql.parquet.compression.codec", "snappy")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    val apps = sc
      .textFile(inputPath)
      .flatMap { line =>
        App.getApp(line)
      }
      .map { x =>
        x.packageName -> x
      }
      .groupByKey()
      .mapValues { values =>
        val data = values.filter(x => x.keywords.size > 0)
        if (data.size > 0) {
          data.head
        } else {
          values.head
        }
      }
      .map(_._2)

    if(apps.count > 50000){
      import spark.implicits._
      apps.toDF().write.mode(SaveMode.Overwrite).parquet(app_data_parquet_path)
    }
    spark.stop()
  }
}
