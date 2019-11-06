package prepare

import com.twitter.scalding.Args
import com.xiaomi.misearch.rank.appstore.common.model.App
import com.xiaomi.misearch.rank.appstore.model.MarketSearchResult
import com.xiaomi.misearch.rank.utils.hdfs.HdfsAccessor
import model.AppStoreContentStatistics
import com.xiaomi.misearch.rank.utils.SerializationUtils
import com.xiaomi.misearch.rank.appstore.common.model
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.MathUtils.computeStandardDeviation
import utils.PathUtils._

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object LabelGenerator {

  val hdfsAccessor = HdfsAccessor.getInstance()

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val start = semanticDate(args.getOrElse("start", "-7"))
    val end = semanticDate(args.getOrElse("end", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var appstoreContentLog = IntermediateDateIntervalPath(appstore_content_stats_path, start, end)
    var appDataPath = app_data_path
    var marketSearchResultPath = market_search_result_path
    var queryDataOutputPath = IntermediateDatePath(query_data_path, end.toInt)
    var labelOutputPath = IntermediateDatePath(label_path, end.toInt)
    var conf = new SparkConf()
      .setAppName(LabelGenerator.getClass.getName)

    if (dev) {
      appstoreContentLog = Seq(appstore_content_stats_path_local)
      appDataPath = app_data_path_local
      marketSearchResultPath = market_search_result_path_local
      queryDataOutputPath = query_data_path_local
      labelOutputPath = label_path_local
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val pnToAppIdMap = getPnToAppIdMap(appDataPath, spark)
    val appIdToDisplayNameMap = getAppIdToDisplayNameMap(appDataPath, spark)
    val downloadData = getDownloadData(appstoreContentLog, spark)
    val marketRankData = getMarketRankData(getMarketSearchResultPath(marketSearchResultPath), pnToAppIdMap, spark)

    val labelData = generate(downloadData, marketRankData, appIdToDisplayNameMap)
    println("Total label data count: " + labelData.count())

    val queries = labelData.map(_._1).distinct()
    val queryData = queries.zipWithIndex().map(x => x._1 -> (x._2 + 1))


    val queryDataSplits = queryData.randomSplit(Array(0.9, 0.1))
    val train = spark.sparkContext.broadcast(queryDataSplits(0).collectAsMap())
    val test = spark.sparkContext.broadcast(queryDataSplits(1).collectAsMap())

    val fs = FileSystem.get(new Configuration())
    val trainOutputPath = labelOutputPath + "/train"
    val testOutputPath = labelOutputPath + "/test"
    fs.delete(new Path(trainOutputPath), true)
    fs.delete(new Path(testOutputPath), true)
    labelData.filter(x => train.value.contains(x._1)).map(_.productIterator.mkString("\t")).repartition(1).saveAsTextFile(trainOutputPath)
    labelData.filter(x => test.value.contains(x._1)).map(_.productIterator.mkString("\t")).repartition(1).saveAsTextFile(testOutputPath)

    fs.delete(new Path(queryDataOutputPath), true)
    spark.sparkContext.makeRDD(queryData.sortBy(_._2).collect().map(_.productIterator.mkString("\t")), 1).repartition(1).saveAsTextFile(queryDataOutputPath)

    spark.stop()
    println("Job done!")
  }

  private def generate(downloadData: RDD[((String, String), (Int, Double, Double, Double))], marketRankData: RDD[((String, String), Double)], appIdToDisplayNameMap: Broadcast[scala.collection.Map[String, String]]) = {
    downloadData.leftOuterJoin(marketRankData).map {
      case ((query, appId), ((downloadCount, downloadRatio, downloadAvg, downloadSd), posAvg)) =>
        var label = 0
        if (posAvg.isDefined) {
          if (posAvg.get == 1 || downloadRatio > 0.5) {
            label = 6
          } else if (posAvg.get >= 3) {
            if (downloadCount >= (downloadAvg + downloadSd)) {
              label = 5
            } else if (downloadCount >= (downloadAvg - downloadSd)) {
              label = 4
            }
          } else if (posAvg.get >= 5) {
            if (downloadCount >= (downloadAvg + downloadSd)) {
              label = 4
            } else if (downloadCount >= (downloadAvg - downloadSd)) {
              label = 3
            }
          }
        }

        if (label == 0) {
          if (downloadCount >= (downloadAvg + downloadSd)) {
            label = 3
          } else if (downloadCount >= (downloadAvg - downloadSd)) {
            label = 2
          } else if (downloadCount > 0) {
            label = 1
          }
        }
        val appTitle = appIdToDisplayNameMap.value.getOrElse(appId, "")
        (query, appId, label, appTitle)
    }.filter(_._3 > 0)
  }

  private def getMarketRankData(marketSearchResultPath: String, pnToAppIdMap: Broadcast[scala.collection.Map[String, String]], spark: SparkSession) = {
    spark.sparkContext.textFile(marketSearchResultPath).flatMap { line =>
      val searchResult = SerializationUtils.fromJson(line, classOf[MarketSearchResult])
      val market = searchResult.getMarket
      val query = searchResult.getQuery
      val apps = searchResult.getResult

      var pos = 0
      val list = for (app <- apps.asScala.take(10)) yield {
        pos = pos + 1
        (normalizeQuery(query), pnToAppIdMap.value.getOrElse(app.getPackageName, ""), market.name(), pos)
      }
      list.filter(x => StringUtils.isNotBlank(x._1) && StringUtils.isNotBlank(x._2))
    }.map(x => (x._1, x._2) -> (x._3, x._4))
      .groupByKey()
      .mapValues {
        it =>
          val posSum = it.map(_._2).sum.toDouble
          val length = it.toList.length
          posSum / length
      }
  }

  def getDownloadData(appstoreContentLog: Seq[String], spark: SparkSession) = {
    import spark.implicits._
    spark.read
      .parquet(appstoreContentLog: _*)
      .as[AppStoreContentStatistics]
      .rdd
      .filter(x => !x.source.isDefined || (x.source.isDefined && x.source.get.equals("应用商店")))
      .filter(x => x.appId.isDefined && x.searchKeyword.isDefined)
      .filter(x => x.ref.isDefined && x.ref.get.equals("search"))
      .filter(x => x.ads.isDefined && x.ads.get == 0)
      .filter(x => x.download.isDefined && x.download.get > 0)
      .map { x =>
        val query = normalizeQuery(x.searchKeyword.get)
        (query, x.appId.get) -> x.download.get
      }
      .filter(x => StringUtils.isNotBlank(x._1._1))
      .reduceByKey(_ + _)
      .map {
        case ((query, appId), download) => (query, (appId.toString, download))
      }
      .groupByKey()
      .filter(_._2.map(_._2).sum > 10)
      .mapValues {
        values =>
          val downloadList = values.map(_._2).toList
          val len = downloadList.length
          val sum = downloadList.sum.toDouble
          val downloadAvg = sum / len
          val downloadSd = computeStandardDeviation(downloadList, downloadAvg, len)

          values.map {
            case (appId, download) => (appId, download, download / sum, downloadAvg, downloadSd)
          }
      }
      .flatMap {
        case (query, data) => {
          data.map(x => (query, x._1) -> (x._2, x._3, x._4, x._5))
        }
      }
  }

  def getPnToAppIdMap(appDataPath: String, spark: SparkSession) = {
    spark.sparkContext.broadcast(spark.sparkContext.textFile(appDataPath).map { line =>
      val app = SerializationUtils.fromJson(line, classOf[App])
      app.getPackageName -> app.getRawId
    }.collectAsMap())
  }

  def getAppIdToDisplayNameMap(appDataPath: String, spark: SparkSession) = {
    spark.sparkContext.broadcast(spark.sparkContext.textFile(appDataPath).map { line =>
      val app = SerializationUtils.fromJson(line, classOf[App])
      app.getRawId -> app.getDisplayName
    }.collectAsMap())
  }

  def getMarketSearchResultPath(path: String): String = {
    val pathRegex = new Regex("search_result_page_(\\d{8})\\.txt")
    val fs = hdfsAccessor.getFileSystem
    val recentDate = fs.listStatus(new Path(path)).filter(_.isFile).map { f =>
      val fileName = f.getPath.getName
      fileName match {
        case pathRegex(date) => Some(date)
        case _ => None
      }
    }.filter(_.isDefined).map(_.get.toLong).max.toString
    String.format(path + "/search_result_page_%s.txt", recentDate)
  }

  def normalizeQuery(word: String): String = {
    word.replace(" ", "").replaceAll("[\\pP‘’“”\t]", "").toLowerCase
  }
}
