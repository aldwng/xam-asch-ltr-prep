package prepare

import com.twitter.scalding.Args
import com.xiaomi.misearch.rank.utils.SerializationUtils
import model.AppRaw
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.PathUtils._

object AppGenerator {

  val LANG = "zh_CN"

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day = semanticDate(args.getOrElse("day", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var categoryPath = category_path
    var inputPath = app_common_path
    var outputPath = IntermediateDatePath(app_data_path, day.toInt)
    var conf = new SparkConf().setAppName(AppGenerator.getClass.getName)
    if (dev) {
      categoryPath = category_path_local
      inputPath = app_common_path_local
      outputPath = app_data_path_local
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val categories = loadCategories(sc, categoryPath)
    val categoryMap = spark.sparkContext.broadcast(categories.collectAsMap())

    val apps = sc
      .textFile(inputPath)
      .flatMap { line =>
        AppRaw.getApp(line, categoryMap)
      }
      .map { x =>
        x.getPackageName -> x
      }
      .groupByKey()
      .mapValues { values =>
        val data = values.filter(x => x.getId().contains(LANG))
        if (data.size > 0) {
          SerializationUtils.toJson(data.head)
        } else {
          SerializationUtils.toJson(values.head)
        }
      }
      .map(_._2)

    println("Total apps count: " + apps.count())
    if (apps.count > 1000) {
      val fs = FileSystem.get(new Configuration())
      fs.delete(new Path(outputPath), true)
      apps.repartition(1).saveAsTextFile(outputPath)
    }
    spark.stop()
    println("Job done!")
  }

  def loadCategories(sc: SparkContext, categoryPath: String): RDD[(Int, String)] = {
    return sc.textFile(categoryPath).map(line => {
      val items = line.split("\t")
      val cateId = items(0).toInt
      val cateName = items(2)
      (cateId, cateName)
    })
  }
}
