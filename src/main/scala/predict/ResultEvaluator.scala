package predict

import com.twitter.scalding.Args
import model.App
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.PathUtils._

import scala.collection.Map


/**
  * @author Shenglan Wang
  */
object ResultEvaluator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day = semanticDate(args.getOrElse("day", "-1"))
    val evaType = args.getOrElse("type", "unified")
    val dev = args.getOrElse("dev", "false").toBoolean

    var downloadHistoryPath = download_history_path
    var appDataPath = app_data_parquet_path

    var rankPath = evaType match {
      case "merged" => IntermediateDatePath(predict_rank_merged_path, day.toInt) + "/rank_merged.txt";
      case _ => IntermediateDatePath(predict_rank_unified_path, day.toInt) + "/part-00000";
    }
    var outputPath = evaType match {
      case "merged" => IntermediateDatePath(predict_evaluate_merged_path, day.toInt)
      case _ => IntermediateDatePath(predict_evaluate_unified_path, day.toInt)
    }

    var conf = new SparkConf().setAppName("Result Evaluator Job")

    if (dev) {
      downloadHistoryPath = download_history_path_local
      appDataPath = app_data_parquet_path_local

      rankPath = evaType match {
        case "merged" => predict_rank_merged_path_local + "/rank_merged.txt";
        case _ => predict_rank_unified_path_local + "/part-00000";
      }
      outputPath = evaType match {
        case "merged" => predict_evaluate_merged_path_local
        case _ => predict_evaluate_unified_path_local
      }
      conf.setMaster("local[*]")
    }

    println("rankPath: " + rankPath)
    println("outputPath: " + outputPath)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val apps = spark.read.parquet(appDataPath).as[App].rdd
    val displayNameMap = apps
      .filter(app => app.displayName.isDefined)
      .map(app => app.rawId -> app.displayName.get)
      .collectAsMap()

    val downloadHisotry = extractRankFile(sc, downloadHistoryPath, displayNameMap)
    val rankResult = extractRankFile(sc, rankPath, displayNameMap)

    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(outputPath), true)

    downloadHisotry.join(rankResult).map {
      case (_, (downloadHistoryLine, rankResultLine)) =>
        val items = downloadHistoryLine.split("\t")
        var searchCount = items(1).toLong
        (searchCount, downloadHistoryLine, rankResultLine)
    }
      .sortBy(-_._1)
      .map {
        case (_, downloadHistoryLine, rankResultLine) =>
          Array(downloadHistoryLine, rankResultLine).mkString("\n")
      }.repartition(1).saveAsTextFile(outputPath)
  }

  private def extractRankFile(sc: SparkContext, rankFile: String, displayNameMap: Map[String, String]): RDD[(String, String)] = {
    val result = sc.textFile(rankFile)
      .map { line =>
        val splits = line.split("\t")

        val query = splits(0)
        val searchCount = splits(1)
        val downloadCount = splits(2)
        val appInfos = splits(3).split(",")

        val appInfosWithDisplayName = appInfos.map { x =>
          val items = x.split(":")
          val appId = items(0)
          val score = items(1).toLong
          val downloadCount = items(2).toLong
          val displayName = displayNameMap.getOrElse(appId, "")

          (appId, displayName, score, downloadCount)
        }.sortBy(-_._3).map(x => x.productIterator.mkString(":"))
        query -> Array(query, searchCount, downloadCount, appInfosWithDisplayName.mkString(",")).mkString("\t")
      }

    result
  }

}
