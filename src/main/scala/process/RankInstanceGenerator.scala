package process

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature.{AppExt, DataRaw, QueryExt, RankInstance}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.PathUtils._

/**
  * Created by yun on 18-1-15.
  */
object RankInstanceGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day = semanticDate(args.getOrElse("day", "-1"))

    val rootPath = IntermediateDatePath(base_path, day.toInt)
    val inputPath = rootPath + "/train/raw/"
    val queryPath = rootPath + "/queryExt"
    val appPath = rootPath + "/appExts"
    val outputPath = rootPath + "/train/instance/"

    val conf = new SparkConf().setAppName("Rank Instance Job")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val appExts = sc.thriftParquetFile(appPath, classOf[AppExt])
    val queryExts = sc.thriftParquetFile(queryPath, classOf[QueryExt])

    val fs = FileSystem.get(new Configuration())
    val paths = Seq("train", "validate", "test")
    paths.foreach { path =>
      val dataRaws = sc.thriftParquetFile(inputPath + path, classOf[DataRaw])
      val instance = generate(sc, dataRaws, appExts, queryExts)
      fs.delete(new Path(outputPath + path), true)
      instance.saveAsParquetFile(outputPath + path)
    }
    spark.stop()
    println("Job done!")
  }

  def generate(sc: SparkContext, dataRaws: RDD[DataRaw], appExts: RDD[AppExt], queryExts: RDD[QueryExt]): RDD[RankInstance] = {
    val appMap = appExts.map(x => x.getAppId -> x).collectAsMap()
    val queryMap = queryExts.map(x => x.getQuery -> x)
    val appHash = sc.broadcast(appMap)

    val output = dataRaws
      .map(x => x.getQuery -> x)
      .leftOuterJoin(queryMap)
      .map {
        case (_, (dataRaw, wrappedQueryExt)) =>
          val ans = new RankInstance()
          ans.setData(dataRaw)
          if (wrappedQueryExt.isDefined) {
            val queryExt = wrappedQueryExt.get
            if (queryExt.isSetExts) {
              ans.setQueryExts(queryExt.getExts)
            }
            if (queryExt.isSetAppIds && queryExt.getAppIdsSize > 0) {
              ans.setQyAppIds(queryExt.getAppIds)
            }
            if (queryExt.isSetExtTfIdf && queryExt.getExtTfIdfSize > 0) {
              ans.setQueryExtTfIdf(queryExt.getExtTfIdf)
            }
            if (queryExt.isSetAppLevel1CategoryName && queryExt.getAppLevel1CategoryNameSize > 0) {
              ans.setQyAppLevel1CategoryName(queryExt.getAppLevel1CategoryName)
            }
            if (queryExt.isSetAppLevel2CategoryName && queryExt.getAppLevel2CategoryNameSize > 0) {
              ans.setQyAppLevel2CategoryName(queryExt.getAppLevel2CategoryName)
            }
            if (queryExt.isSetAppTags && queryExt.getAppTagsSize > 0) {
              ans.setQyAppTags(queryExt.getAppTags)
            }
            if (queryExt.isSetKeywords && queryExt.getKeywordsSize > 0) {
              ans.setQyKeywords(queryExt.getKeywords)
            }
            if (queryExt.isSetSearchKeywords && queryExt.getSearchKeywordsSize > 0) {
              ans.setQySearchKeywords(queryExt.getSearchKeywords)
            }
            if (queryExt.isSetFolderTags && queryExt.getFolderTagsSize > 0) {
              ans.setQyFolderTags(queryExt.getFolderTags)
            }
            if (queryExt.isSetRelatedTags && queryExt.getRelatedTagsSize > 0) {
              ans.setQyRelatedTags(queryExt.getRelatedTags)
            }

            if (queryExt.isSetDisplayNames && queryExt.getDisplayNamesSize > 0) {
              ans.setQyDisplayNames(queryExt.getDisplayNames)
            }

            if (queryExt.isSetPublisher && queryExt.getPublisherSize > 0) {
              ans.setQyPublisher(queryExt.getPublisher)
            }

            if (queryExt.isSetGoogleCates && queryExt.getGoogleCatesSize > 0) {
              ans.setGoogleCates(queryExt.getGoogleCates)
            }
          }
          if (appHash.value.contains(dataRaw.getAppId)) {
            ans.setApp(appHash.value(dataRaw.getAppId))
          }
          ans
      }
    output
  }

}
