package base

import com.twitter.scalding.Args
import model.App
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.PathUtils._

object AppGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val dev   = args.getOrElse("dev", "false").toBoolean

    var inputPath = app_data_path
    var outputPath = app_data_parquet_path
    var conf = new SparkConf().setAppName(AppGenerator.getClass.getName).set("spark.sql.parquet.compression.codec", "snappy")
    if (dev) {
      inputPath = app_data_path_local
      outputPath = app_data_parquet_path_local
      conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName(AppGenerator.getClass.getName)
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
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

    if (apps.count > 1000) {
      apps.toDF().write.mode(SaveMode.Overwrite).parquet(outputPath)
    }
    spark.stop()
  }
}
