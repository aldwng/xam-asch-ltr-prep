package base

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature._
import model.{App, AppStoreContentStatistics}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.FilterWords
import utils.PathUtils._
import utils.ProcessUtils._

import scala.collection.JavaConverters._

object AppExtGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val yday   = semanticDate(args.getOrElse("yday", "-1"))
    val month = semanticDate(args.getOrElse("month", "-30"))

    val appExtPath = base_path + "/appBase"

    val conf = new SparkConf()
      .setAppName("App Ext Job")
      .set("spark.sql.parquet.compression.codec", "snappy")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val lastMonth   = IntermediateDateIntervalPath(appstore_content_stats_path, month, yday)
    val coClickQueries = calcCoClickQueryTfIdf(spark, lastMonth)

    import spark.implicits._
    val apps = spark.read.parquet(app_data_parquet_path).as[App].rdd

    val appExts = generate(spark, apps, coClickQueries)
    if (appExts.count() > 50000) {
      val fs = FileSystem.get(new Configuration())
      fs.delete(new Path(appExtPath), true)
      appExts.saveAsParquetFile(appExtPath)
    }
    spark.stop()
  }

  def generate(spark: SparkSession, apps: RDD[App], coClickQueries: RDD[(Long, Seq[String])]): RDD[AppExt] = {
    val coClickQueryMap  = spark.sparkContext.broadcast(coClickQueries.collectAsMap())

    return apps.map(app => {
      val appExt = new AppExt()
      val appId = app.rawId.toLong
      appExt.setAppId(appId)
      appExt.setPackageName(app.packageName)
      if (app.displayName.isDefined) {
        val displayName = app.displayName.get
        appExt.setDisplayName(displayName)

        val displayNameSeg = wordSegment(displayName)
        if (displayNameSeg.size > 0) {
          appExt.setDisplayNameSeg(displayNameSeg.asJava)
        }
      }

      if (app.introduction.isDefined) {
        val desc = app.introduction.get
        appExt.setDesc(desc)

        val descSeg = textSegment(desc)
        if (descSeg.size > 0) {
          appExt.setDescSeg(tokenize(descSeg).asJava)
        }
      }

      if (app.brief.isDefined) {
        val brief = app.brief.get
        appExt.setBrief(brief)

        val briefSeg = textSegment(brief)
        if (briefSeg.size > 0) {
          appExt.setBriefSeg(tokenize(briefSeg).asJava)
        }
      }

      if (app.publisher.isDefined) {
        appExt.setPublisher(app.publisher.get)
      }

      //        appExt.setLevel1CategoryName()
      //        appExt.setLevel2CategoryName()
      //        appExt.setTags()
      //        appExt.setDeveloperAppCount()
      //        appExt.setApkSize()
      //        appExt.setRankOrder()
      //        appExt.setRankOrderForPad()
      //        appExt.setLastWeekUpdateCount()
      //        appExt.setLastMonthUpdateCount()
      //        appExt.setFavoriteCount()
      //        appExt.setFeedbackCount()
      //        appExt.setPermissionCount()
      //        appExt.setCreateTime()

      if (app.appActiveRank.isDefined && app.appActiveRank.get != 0) {
        appExt.setAppActiveRank(app.appActiveRank.get)
      }
      if (app.appCdr.isDefined && app.appCdr.get != 0) {
        appExt.setAppCdr(app.appCdr.get)
      }

      if (app.appDownloadRank.isDefined && app.appDownloadRank.get != 0) {
        appExt.setAppDownloadRank(app.appDownloadRank.get)
      }

      if (app.appHot.isDefined && app.appHot.get != 0) {
        appExt.setAppHot(app.appHot.get)
      }

      if (app.appRank.isDefined && app.appRank.get != 0) {
        appExt.setAppRank(app.appRank.get)
      }

      if (app.folderTags.nonEmpty) {
        appExt.setFolderTags(app.folderTags.asJava)
      }

      if (app.gameArpu.isDefined && app.gameArpu.get != 0) {
        appExt.setGameArpu(app.gameArpu.get)
      }

      if (app.gameCdr.isDefined && app.gameCdr.get != 0) {
        appExt.setGameCdr(app.gameCdr.get)
      }

      if (app.gameRank.isDefined && app.gameRank.get != 0) {
        appExt.setGameRank(app.gameRank.get)
      }

      if (app.ratingScore.isDefined && app.ratingScore.get != 0) {
        appExt.setRatingScore(app.ratingScore.get)
      }

      if (app.ratingScore.isDefined && app.ratingScore.get != 0) {
        appExt.setRatingScore(app.ratingScore.get)
      }

      if (app.keywords.nonEmpty) {
        appExt.setKeywords(app.keywords.asJava)
      }

      if (app.relatedTags.nonEmpty) {
        appExt.setRelatedTags(app.relatedTags.asJava)
      }

      if (app.searchKeywords.nonEmpty) {
        appExt.setSearchKeywords(app.searchKeywords.asJava)
      }

      if (app.wdjCategory.isDefined) {
        appExt.setWdjCategory(app.wdjCategory.get)
      }

      if (coClickQueryMap.value.contains(appId)) {
        appExt.setCoclickTfIdf(coClickQueryMap.value(appId).asJava)
      }

      appExt
    })
  }

  def calcCoClickQueryTfIdf(spark: SparkSession, inputPaths: Seq[String]) = {
    import spark.implicits._
    val input = spark.read
      .parquet(inputPaths: _*)
      .as[AppStoreContentStatistics]
      .rdd
      .filter(x => !x.source.isDefined || (x.source.isDefined && x.source.get.equals("应用商店")))
      .filter(x => x.ref.isDefined && x.ref.get.equals("search"))
      .filter(x => x.ads.isDefined && x.ads.get == 0)
      .filter(x => x.appId.isDefined && x.searchKeyword.isDefined)
      .filter(x => !FilterWords.isNumber(x.searchKeyword))
      .filter(x => !FilterWords.isUrl(x.searchKeyword))
      .filter(x => x.download.isDefined && x.download.get > 0)
      .map(x => x.appId.get -> parseQuery(x.searchKeyword.get))
      .filter(x => nonBlank(x._2))
      .groupByKey()
      .map(x => x._1 -> x._2.toSeq.distinct)
      .filter(x => x._2.size > 1 && x._2.size <= 100)

    val output = input.map {
      case (appId, queries) =>
        val terms = wordSegment(queries.mkString(" "))
        val qys = tokenize(terms)
          .map(x => x -> 1)
          .groupBy(_._1)
          .map(x => (x._1, x._2.size))
          .toSeq
          .sortBy(-_._2)
          .map(_._1)
          .take(10)
        appId -> qys
    }
    output
  }


}
