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
    var reRankPath = IntermediateDatePath(predict_rerank_path, day.toInt)
    var outputPath = IntermediateDatePath(predict_download_history_path, day.toInt)

    var conf = new SparkConf().setAppName("Download History Rerank Job Job")

    if(dev) {
      downloadHistoryPath = download_history_path_local
      reRankPath =  predict_rerank_path_local
      outputPath = predict_download_history_path_local
      conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext


    val downloadHistory = sc.textFile(downloadHistoryPath)
      .map(line => line.split("\t")(0) -> line)

    val naturalReRank = sc.textFile(reRankPath)
      .map(line => line.split(",")(0) -> line)

    val result = downloadHistory.join(naturalReRank)
      .map {
        case (query, (downloadsLine, rerankLine)) =>
          val downloadsItems = downloadsLine.split("\t")
          val searchCount = downloadsItems(1)
          val downloadCount = downloadsItems(2)
          val appDownloadsMap = downloadsItems(3).split(",")
            .map { line =>
              val items = line.split("\\:")
              (items(0), items(2))
            }.toMap
          val appReRankList = rerankLine.split(",")(1).split(";")
            .map { line =>
              val items = line.split("\\:")
              val appId = items(0)
              val rankScore = items(1).toDouble
              val downloads = appDownloadsMap.getOrElse(appId, 0)
              (appId, rankScore, downloads, rankScore)
            }.sortBy(-_._2)
          val test = appReRankList.map(tuple =>
            tuple.productIterator.mkString(":"))
          Array(query, searchCount, downloadCount, test.mkString(",")).mkString("\t")
      }

    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(outputPath), true)

    result.repartition(1)
      .saveAsTextFile(outputPath)
  }
}
