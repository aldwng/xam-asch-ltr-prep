package tool

import com.twitter.scalding.Args
import model.AppRaw
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.PathUtils._

/**
  * Compare two rank results, params
  * --dev false/false
  * --ranka rank a filename
  * --rankb rank b filename
  *
  * @author Shenglan Wang
  */
object RankComparator {

  val LANG = "zh_CN"

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val dev = args.getOrElse("dev", "false").toBoolean
    val rankAFilename = args.getOrElse("ranka", "")
    val rankBFilename = args.getOrElse("rankb", "")

    var conf = new SparkConf().setAppName(RankComparator.getClass.getName)
    var categoryPath = category_path
    var appCommonPath = app_common_path
    var rankAFile = base_path + "/" + rankAFilename
    var rankBFile = base_path + "/" + rankBFilename
    var outputPath = rank_diff_path

    if (dev) {
      categoryPath = category_path_local
      appCommonPath = app_common_path_local
      rankAFile = base_path_local + "/" + rankAFilename
      rankBFile = base_path_local + "/" + rankBFilename
      outputPath = rank_diff_path_local
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val categories = loadCategories(sc, categoryPath)
    val categoryMap = spark.sparkContext.broadcast(categories.collectAsMap())
    val appIdToTitleMap = loadAppIdToTitleMap(sc, appCommonPath, categoryMap)

    val rankAData = parseRankResultFile(sc, rankAFile, appIdToTitleMap)
    val rankBData = parseRankResultFile(sc, rankBFile, appIdToTitleMap)

    val result = rankAData.join(rankBData).map {
      case (_, (data1, data2)) =>
        (data1, data2).productIterator.mkString("\n")
    }

    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(outputPath), true)
    result.coalesce(1).saveAsTextFile(outputPath)
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

  def loadAppIdToTitleMap(sc: SparkContext, appCommonPath: String, categoryMap: Broadcast[scala.collection.Map[Int, String]]) = {
    val appIdToTitle = sc.textFile(appCommonPath)
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
          data.head
        } else {
          values.head
        }
      }
      .map(_._2)
      .map(x => x.getRawId -> x.getDisplayName)

    sc.broadcast(appIdToTitle.collectAsMap())
  }

  def parseRankResultFile(sc: SparkContext, rankFilePath: String, appIdToTitleMap: Broadcast[scala.collection.Map[String, String]]) = {
    sc.textFile(rankFilePath).map(line => {
      val items = line.split(":")
      val query = items(0)
      val ids = items(1).split(",").take(10)
      val titles = for (id <- ids) yield {
        appIdToTitleMap.value.getOrElse(id, "")
      }
      query -> (query, titles.mkString(", ")).productIterator.mkString("\t")
    })
  }
}

