package predict

import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.PathUtils._

object ResultMerger {
  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day = semanticDate(args.getOrElse("day", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean
    var downloadHistoryPath = download_history_path
    var rankPath = IntermediateDatePath(predict_rank_path, day.toInt)
    var outputPath = IntermediateDatePath(predict_rank_unified_path, day.toInt)

    var conf = new SparkConf().setAppName("Download History Rerank Job Job")

    if(dev) {
      downloadHistoryPath = download_history_path_local
      rankPath =  predict_rank_path_local
      outputPath =  predict_rank_unified_path_local
      conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext


    val downloadHistory = sc.textFile(downloadHistoryPath)
      .map(line => line.split("\t")(0) -> line)

    val predictRank = sc.textFile(rankPath)
      .map(line => line.split(",")(0) -> line)

    val result = downloadHistory.join(predictRank)
      .map {
        case (query, (downloadHistoryLine, predictRankLine)) =>
          val downloadHistoryItems = downloadHistoryLine.split("\t")
          val searchCount = downloadHistoryItems(1)
          val downloadCount = downloadHistoryItems(2)
          val appDownloadMap = downloadHistoryItems(3).split(",")
            .map { line =>
              val items = line.split(":")
              val appId = items(0)
              val downloadCount = items(2)
              appId -> downloadCount
            }.toMap

          val predictRankItems = predictRankLine.split(",")(1).split(";")
            .map { line =>
              val items = line.split(":")
              val appId = items(0)
              val rankScore = items(1).toDouble
              val downloadCount = appDownloadMap.getOrElse(appId, 0)
              (appId, rankScore, downloadCount, rankScore)
            }.sortBy(-_._2)

          val appInfos = predictRankItems.map(tuple =>
            tuple.productIterator.mkString(":"))
          Array(query, searchCount, downloadCount, appInfos.mkString(",")).mkString("\t")
      }

    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(outputPath), true)

    result.repartition(1)
      .saveAsTextFile(outputPath)
  }
}
