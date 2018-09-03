package sample

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import utils.FeatureUtils._
import utils.PathUtils._
import utils.TextUtils._

import scala.collection.JavaConverters._

object NaturalSampleGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day = semanticDate(args.getOrElse("day", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var queryPath = IntermediateDatePath(query_ext_path, day.toInt)
    var appPath = IntermediateDatePath(app_ext_parquet_path, day.toInt)
    var queryMapPath = IntermediateDatePath(predict_query_map_path, day.toInt)
    var naturalResultsPath = natural_results_path
    var outputPath = IntermediateDatePath(predict_base_path, day.toInt)
    var conf = new SparkConf().setAppName("Natural Result Rank Job")

    if (dev) {
      queryPath = query_ext_path_local
      appPath = app_ext_parquet_path_local
      queryMapPath = predict_query_map_path_local
      naturalResultsPath = natural_results_path_local
      outputPath = predict_base_path_local
      conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val naturalResults = sc
      .textFile(naturalResultsPath)
      .zipWithIndex()
      .map {
        case (text, id) =>
          val items = text.split("\t")
          val query = items(0)
          val appIds = items(1).split(",").toSeq
          (query, id, appIds)
      }
      .filter(naturalResult => nonBlank(naturalResult._1))

    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(queryMapPath), true)
    naturalResults.map {
      case (query, id, _) =>
        val ans = new QueryMap()
        ans.setQuery(query)
        ans.setId(id)
        ans
    }.saveAsParquetFile(queryMapPath)

    val output = generate(sc, appPath, queryPath, naturalResults)
    fs.delete(new Path(outputPath), true)
    output.saveAsParquetFile(outputPath)

    spark.stop()
    println("Job done!")
  }

  def generate(sc: SparkContext, appPath: String, queryExtPath: String, naturalResults: RDD[(String, Long, Seq[String])]): RDD[Sample] = {
    val appExts = sc.thriftParquetFile(appPath, classOf[AppExt]).map(x => x.getAppId -> x).collectAsMap()
    val queryExts = sc.thriftParquetFile(queryExtPath, classOf[QueryExt])
      .map { x =>
        x.getQuery -> x
      }
    val appHash = sc.broadcast(appExts)

    val output = naturalResults
      .map(x => x._1 -> x._3)
      .join(queryExts)
      .flatMap {
        case (qy, (naturalResultApps, queryExt)) =>
          val seg = wordSegment(qy)
          naturalResultApps
            .map { appIdStr =>
              val appId = appIdStr.toLong
              val raw = new DataRaw()
              raw.setAppId(appId)
              raw.setQuery(qy)
              if (seg.nonEmpty) {
                raw.setQuerySeg(seg.asJava)
              }
              val ins = new RankInstance()
              ins.setData(raw)
              if (queryExt.isSetExts) {
                ins.setQueryExts(queryExt.getExts)
              }
              if (queryExt.isSetAppIds && queryExt.getAppIdsSize > 0) {
                ins.setQyAppIds(queryExt.getAppIds)
              }
              if (queryExt.isSetExtTfIdf && queryExt.getExtTfIdfSize > 0) {
                ins.setQueryExtTfIdf(queryExt.getExtTfIdf)
              }
              if (queryExt.isSetAppLevel1CategoryName && queryExt.getAppLevel1CategoryNameSize > 0) {
                ins.setQyAppLevel1CategoryName(queryExt.getAppLevel1CategoryName)
              }
              if (queryExt.isSetAppLevel2CategoryName && queryExt.getAppLevel2CategoryNameSize > 0) {
                ins.setQyAppLevel2CategoryName(queryExt.getAppLevel2CategoryName)
              }
              if (queryExt.isSetAppTags && queryExt.getAppTagsSize > 0) {
                ins.setQyAppTags(queryExt.getAppTags)
              }
              if (queryExt.isSetKeywords && queryExt.getKeywordsSize > 0) {
                ins.setQyKeywords(queryExt.getKeywords)
              }
              if (queryExt.isSetSearchKeywords && queryExt.getSearchKeywordsSize > 0) {
                ins.setQySearchKeywords(queryExt.getSearchKeywords)
              }
              if (queryExt.isSetFolderTags && queryExt.getFolderTagsSize > 0) {
                ins.setQyFolderTags(queryExt.getFolderTags)
              }
              if (queryExt.isSetRelatedTags && queryExt.getRelatedTagsSize > 0) {
                ins.setQyRelatedTags(queryExt.getRelatedTags)
              }

              if (queryExt.isSetDisplayNames && queryExt.getDisplayNamesSize > 0) {
                ins.setQyDisplayNames(queryExt.getDisplayNames)
              }

              if (queryExt.isSetPublisher && queryExt.getPublisherSize > 0) {
                ins.setQyPublisher(queryExt.getPublisher)
              }
              if (appHash.value.contains(appId)) {
                ins.setApp(appHash.value(appId))
              }
              ins
            }
            .filter(x => x.isSetApp && x.isSetData)
            .map { ins =>
              val features = extractFeatures(ins)
              val label = ins.getData.isSetLabel match {
                case true => ins.getData.getLabel
                case false => 0
              }
              val ans = new Sample()
              ans.setQuery(ins.getData.getQuery)
              ans.setLabel(label)
              ans.setFeatures(features.asJava)
              ans.setCommon(ins.getApp.getAppId.toString)
              ans
            }
      }
    output
  }


}
