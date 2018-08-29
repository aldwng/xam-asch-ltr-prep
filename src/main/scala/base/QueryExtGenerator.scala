package base

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature.{AppExt, QueryExt, QueryExtItem}
import model.AppStoreContentStatistics
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.PathUtils._
import utils.TextUtils._
import utils.MathUtils._

import scala.collection.JavaConverters._

object QueryExtGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val start = semanticDate(args.getOrElse("start", "-7"))
    val end = semanticDate(args.getOrElse("end", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var inputPaths = IntermediateDateIntervalPath(appstore_content_stats_path, start, end)
    var appExtPath = IntermediateDatePath(app_ext_parquet_path, end.toInt)
    var outputPath = IntermediateDatePath(query_ext_path, end.toInt)
    var conf = new SparkConf().setAppName(QueryExtGenerator.getClass.getName)

    if (dev) {
      inputPaths = Seq(appstore_content_stats_path_local)
      appExtPath = app_ext_parquet_path_local
      outputPath = query_ext_path_local
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val fs = FileSystem.get(new Configuration())
    val sc = spark.sparkContext
    val appExts = sc.thriftParquetFile(appExtPath, classOf[AppExt])

    val data = generate(spark, inputPaths, appExts)
    fs.delete(new Path(outputPath), true)
    data.saveAsParquetFile(outputPath)

    spark.stop()
    println("Job done!")
  }

  def generate(spark: SparkSession, inputPaths: Seq[String], appExts: RDD[AppExt]): RDD[QueryExt] = {
    val apps = appExts
      .map { app =>
        app.getAppId -> app
      }

    val appNameToTagsMap = appExts
      .filter(x => x.isSetDisplayName && x.getDisplayName.nonEmpty)
      .map { x =>
        val appIds = Seq(x.getAppId)
        val tags = x.isSetTags match {
          case true => x.getTags.asScala
          case _ => Seq.empty
        }
        val keywords = x.isSetKeywords match {
          case true => x.getKeywords.asScala
          case _ => Seq.empty
        }
        val cate1 = x.isSetLevel1CategoryName match {
          case true => Seq(x.getLevel1CategoryName)
          case _ => Seq.empty
        }

        val cate2 = x.isSetLevel2CategoryName match {
          case true => Seq(x.getLevel2CategoryName)
          case _ => Seq.empty
        }

        val publisher = x.isSetPublisher match {
          case true => Seq(x.getPublisher)
          case _ => Seq.empty
        }

        val relatedTags = x.isSetRelatedTags match {
          case true => x.getRelatedTags.asScala
          case _ => Seq.empty
        }

        val folderTags = x.isSetFolderTags match {
          case true => x.getFolderTags.asScala
          case _ => Seq.empty
        }
        val searchKeywords = x.isSetSearchKeywords match {
          case true => x.getSearchKeywords.asScala
          case _ => Seq.empty
        }

        val googleCates = x.isSetGoogleCates match {
          case true => x.getGoogleCates.asScala.map(_.getId)
          case _ => Seq.empty
        }

        x.getDisplayName.toLowerCase() -> (appIds, tags, keywords, cate1, cate2, publisher, relatedTags, folderTags, searchKeywords, googleCates)

      }
      .reduceByKey((l, r) => (l._1 ++ r._1, l._2 ++ r._2, l._3 ++ r._3, l._4 ++ r._4, l._5 ++ r._5, l._6 ++ r._6, l._7 ++ r._7, l._8 ++ r._8, l._9 ++ r._9, l._10 ++ r._10))
      .mapValues(x => (x._1.distinct, x._2.distinct, x._3.distinct, x._4.distinct, x._5.distinct, x._6.distinct, x._7.distinct, x._8.distinct, x._9.distinct, x._10.distinct))
      .collectAsMap()

    val broadCastedAppNameToTagsMap = spark.sparkContext.broadcast(appNameToTagsMap)

    import spark.implicits._
    val data = spark.read
      .parquet(inputPaths: _*)
      .as[AppStoreContentStatistics]
      .rdd
      .filter(x => !x.source.isDefined || (x.source.isDefined && x.source.get.equals("应用商店")))
      .filter(x => x.appId.isDefined && x.searchKeyword.isDefined)
      .filter(x => x.ref.isDefined && x.ref.get.equals("search"))
      .filter(x => x.ads.isDefined && x.ads.get == 0)
      .filter(x => x.download.isDefined && x.download.get > 0)
      .map { x =>
        val query = parseQuery(x.searchKeyword.get)
        (query, x.appId.get) -> x.download.get
      }
      .filter(x => nonBlank(x._1._1))
      .reduceByKey(_ + _)
      .filter(x => x._2 > 0)

    val queries = spark.sparkContext
      .parallelize(
        data
          .map(x => x._1._1 -> x._2)
          .reduceByKey(_ + _)
          .sortBy(_._2, false)
          .map(_._1)
          .take(5000000))
      .map { query =>
        val queryExt = new QueryExt()
        queryExt.setQuery(query)
        query -> queryExt
      }

    val queryTags = data
      .map { x =>
        x._1._2 -> x._1._1
      }
      .join(apps)
      .map {
        case (_, (query, app)) =>
          val displayName = if (app.isSetDisplayName && nonBlank(app.getDisplayName)) {
            app.getDisplayName
          } else {
            ""
          }
          val cate1 = if (app.isSetLevel1CategoryName) {
            app.getLevel1CategoryName
          } else {
            ""
          }
          val cate2 = if (app.isSetLevel2CategoryName) {
            app.getLevel2CategoryName
          } else {
            ""
          }
          val tags = if (app.isSetTags) {
            app.getTags.asScala
          } else {
            Seq.empty
          }

          val keywords = if (app.isSetKeywords) {
            app.getKeywords.asScala
          } else {
            Seq.empty
          }
          val publisher = if (app.isSetPublisher) {
            app.getPublisher
          } else {
            ""
          }

          val folderTags = if (app.isSetFolderTags) {
            app.getFolderTags.asScala
          } else {
            Seq.empty
          }
          val relatedTags = if (app.isSetRelatedTags) {
            app.getRelatedTags.asScala
          } else {
            Seq.empty
          }

          val searchKeywords = if (app.isSetSearchKeywords) {
            app.getSearchKeywords.asScala
          } else {
            Seq.empty
          }

          val googleCates = if (app.isSetGoogleCates) {
            app.getGoogleCates.asScala.map(_.getId)
          } else {
            Seq.empty
          }

          query -> (app.getAppId, cate1, cate2, tags, keywords, publisher, folderTags, relatedTags, searchKeywords, googleCates, displayName)
      }
      .groupByKey()
      .mapValues { values =>
        val appIds = values.map(_._1).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        val cate1 = values.map(_._2).filter(c => nonBlank(c)).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(1).map(_._1)
        val cate2 = values.map(_._3).filter(c => nonBlank(c)).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(2).map(_._1)
        val tags = values.flatMap(_._4).filter(nonBlank(_)).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        val keywords = values.flatMap(_._5).filter(nonBlank(_)).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        val publisher = values.map(_._6).filter(p => nonBlank(p)).toSeq.distinct
        val folderTags = values.flatMap(_._7).filter(nonBlank(_)).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(5).map(_._1)
        val relatedTags = values.flatMap(_._8).filter(nonBlank(_)).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        val searchKeywords = values.flatMap(_._9).filter(nonBlank(_)).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        val googleCates = values.flatMap(_._10).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(5).map(_._1)
        val displayNames = values.map(_._11).filter(nonBlank(_)).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        (appIds, cate1, cate2, tags, keywords, publisher, folderTags, relatedTags, searchKeywords, googleCates, displayNames)
      }

    val coClickQueryRaw = data
      .map {
        case ((query, appId), download) =>
          appId -> (query, download)
      }
      .groupByKey()
      .values
      .flatMap { x =>
        x.toSeq.sortBy(-_._2).sliding(2, 1).flatMap { it =>
          val item1 = it.head
          val item2 = it.last
          Seq((item1._1, item2._1) -> item2._2, (item2._1, item1._1) -> item1._2)
        }
      }
      .reduceByKey(_ + _)
      .map {
        case ((a, b), num) =>
          a -> (b, num)
      }
      .groupByKey()
      .mapValues(x => x.toSeq.filter(c => c._2 >= 2).sortBy(-_._2).take(50))
      .filter(x => x._2.size > 0)

    val tfIdfMap = calcQueryExtsTfIdf(coClickQueryRaw).collectAsMap()

    val coClickQueryExtItems = coClickQueryRaw
      .mapValues { v =>
        val coClickQueries = v.take(10)
        val numList = coClickQueries.map(_._2)
        val len = numList.length
        val avg = numList.sum.toDouble / len
        val sd = computeStandardDeviation(numList, avg, len) + 1.0
        coClickQueries.filter(e => !e._1.matches("[a-zA-Z0-9]+")).map { e =>
          val weight = (e._2 - avg) / sd
          val item = new QueryExtItem()
          item.setQuery(e._1)
          item.setWeight(weight)

          val queryExtSeg = wordSegment(e._1)
          if (queryExtSeg.size > 0) {
            item.setQuerySeg(queryExtSeg.asJava)
          }
          item
        }
      }
      .filter(x => x._2.size > 0)

    val output = queries
      .leftOuterJoin(coClickQueryExtItems)
      .flatMap {
        case (query, (queryExt, queryExtItems)) =>
          var querySeq = Seq(query)
          if (queryExtItems.isDefined) {
            queryExt.setExts(queryExtItems.get.asJava)
            querySeq = querySeq ++ queryExtItems.get.map(_.getQuery)
          }
          if (tfIdfMap.contains(query)) {
            queryExt.setExtTfIdf(tfIdfMap(query).asJava)
          }
          querySeq.map { query =>
            query -> queryExt
          }
      }
      .join(queryTags)
      .map {
        case (_, (queryExt, tags)) =>
          queryExt.getQuery -> (queryExt, tags)
      }
      .groupByKey()
      .map { x =>
        val query = x._1
        val queryExt = x._2.head._1
        val values = x._2.map(_._2).toSeq
        var appIds = values.flatMap(_._1).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        var cate1 = values.flatMap(_._2).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(2).map(_._1)
        var cate2 = values.flatMap(_._3).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(4).map(_._1)
        var tags = values.flatMap(_._4).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        var keywords = values.flatMap(_._5).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        var publisher = values.flatMap(_._6).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        var folderTags = values.flatMap(_._7).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(10).map(_._1)
        var relatedTags = values.flatMap(_._8).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        var searchKeywords = values.flatMap(_._9).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1)
        var googleCates = values.flatMap(_._10).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(5).map(_._1)
        var displayNames = values.flatMap(_._11).map(_ -> 1).groupBy(_._1).toSeq.sortBy(-_._2.size).take(20).map(_._1).map(_.toLowerCase)

        if ((!displayNames.contains(query)) && broadCastedAppNameToTagsMap.value.contains(query)) {
          val (appIdsNew, tagsNew, keywordsNew, cate1New, cate2New, publisherNew, relatedTagsNew, folderTagsNew, searchKeywordsNew, googleCatesNew) = broadCastedAppNameToTagsMap.value(query)
          appIds = (appIdsNew ++ appIds).distinct
          cate1 = (cate1New ++ cate1).distinct
          cate2 = (cate2New ++ cate2).distinct
          tags = (tagsNew ++ tags).distinct
          keywords = (keywordsNew ++ keywords).distinct
          publisher = (publisherNew ++ publisher).distinct
          folderTags = (folderTagsNew ++ folderTags).distinct
          relatedTags = (relatedTagsNew ++ relatedTags).distinct
          searchKeywords = (searchKeywordsNew ++ searchKeywords).distinct
          googleCates = (googleCatesNew ++ googleCates).distinct
          displayNames = query +: displayNames
        }

        if (appIds.nonEmpty) {
          queryExt.setAppIds(appIds.map(long2Long).asJava)
        }
        if (cate1.nonEmpty) {
          queryExt.setAppLevel1CategoryName(cate1.asJava)
        }
        if (cate2.nonEmpty) {
          queryExt.setAppLevel2CategoryName(cate2.asJava)
        }
        if (tags.nonEmpty) {
          queryExt.setAppTags(tags.asJava)
        }
        if (keywords.nonEmpty) {
          queryExt.setKeywords(keywords.asJava)
        }
        if (publisher.nonEmpty) {
          queryExt.setPublisher(publisher.asJava)
        }
        if (folderTags.nonEmpty) {
          queryExt.setFolderTags(folderTags.asJava)
        }
        if (relatedTags.nonEmpty) {
          queryExt.setRelatedTags(relatedTags.asJava)
        }
        if (searchKeywords.nonEmpty) {
          queryExt.setSearchKeywords(searchKeywords.asJava)
        }
        if (googleCates.nonEmpty) {
          queryExt.setGoogleCates(googleCates.map(c => Integer.valueOf(c)).asJava)
        }
        if (displayNames.nonEmpty) {
          queryExt.setDisplayNames(displayNames.asJava)
        }
        queryExt
      }
      .filter(x => (x.isSetExts || x.isSetAppTags || x.isSetKeywords || x.isSetRelatedTags || x.isSetAppLevel1CategoryName || x.isSetDisplayNames))

    output
  }

  def calcQueryExtsTfIdf(data: RDD[(String, Seq[(String, Int)])]) = {
    val output = data.map {
      case (query, coClickQueries) =>
        val terms = wordSegment(coClickQueries.map(_._1).mkString(" ") + " " + query)
        val coClickQueryTerms = tokenize(terms)
          .filter(nonNumericRegex.pattern.matcher(_).matches)
          .map(x => x -> 1)
          .groupBy(_._1)
          .map(x => (x._1, x._2.size))
          .toSeq
          .sortBy(-_._2)
          .map(_._1)
          .take(10)
        query -> coClickQueryTerms
    }
    output
  }
}
