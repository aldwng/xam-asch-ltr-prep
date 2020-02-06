package com.xiaomi.misearch.rank.music.prepare.artist

import com.xiaomi.misearch.rank.music.common.model.artist._
import com.xiaomi.misearch.rank.music.prepare.artist.FeatureGenerator._
import com.xiaomi.misearch.rank.music.prepare.artist.LabelGenerator._
import com.xiaomi.misearch.rank.music.utils.Paths._
import com.xiaomi.misearch.rank.utils.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer


object SampleGenerator {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    val sparkContext = new SparkContext(sparkConf)

    var musicsWithStats = generateMusicStatsFeature(sparkContext)

    val musicIdSet = sparkContext.broadcast(musicsWithStats.map(_._1).collect().toSet)

    val musicInfoMap = sparkContext.broadcast(getMusicDataInfo(sparkContext, musicIdSet).collectAsMap())

    musicsWithStats = addMusicInfoToMusicItems(musicsWithStats, musicInfoMap)

    val queryStats = generateQueryStatsFeature(sparkContext)
    val queryStatsMap = sparkContext.broadcast(queryStats.collectAsMap())

    musicsWithStats = addQueryStatsToMusicItems(musicsWithStats, queryStatsMap)

    val artistWithMusicItems = transformToArtistWithMusicItems(musicsWithStats)

    val artistWithLabelAndFeatures = generateLabel(artistWithMusicItems)

    val artistModelSamples = generateSamples(artistWithLabelAndFeatures)

    val trainModelSamples = artistModelSamples._1
    val testModelSamples = artistModelSamples._2

    val samplePath = addDateTimeToDir(ARTIST_SAMPLE_PATH, 1)
    val trainOutputPath = samplePath + "/train"
    val testOutputPath = samplePath + "/test"

    val hadoopConf = new Configuration(sparkContext.hadoopConfiguration)
    val fileSystem: FileSystem = FileSystem.get(hadoopConf)

    fileSystem.delete(new Path(trainOutputPath), true)
    trainModelSamples.map(ArtistModelUtils.convertToText).saveAsTextFile(trainOutputPath)
    fileSystem.delete(new Path(testOutputPath), true)
    testModelSamples.map(ArtistModelUtils.convertToText).saveAsTextFile(testOutputPath)

    val storedQueryStats = queryStats
      .map {
        case ((song, artist), count) =>
          SerializationUtils.toJson(new ArtistStoredQueryItem(song, artist, count))
      }

    val storedStatsItems = musicsWithStats
      .map {
        case (_, musicItem) =>
          SerializationUtils.toJson(new ArtistStoredStatsItem(musicItem))
      }

    fileSystem.delete(new Path(STORED_QUERY_STATS), true)
    storedQueryStats.repartition(1).saveAsTextFile(STORED_QUERY_STATS)
    fileSystem.delete(new Path(STORED_STATS_ITEMS), true)
    storedStatsItems.repartition(1).saveAsTextFile(STORED_STATS_ITEMS)

    sparkContext.stop()
  }

  private def generateSamples(rawData: RDD[(String, ListBuffer[(ArtistMusicItem, Int)])]):
  (RDD[ArtistSample], RDD[ArtistSample]) = {

    val artistWithSamples = rawData.zipWithIndex()
      .map {
        case ((artist, items), index) =>
          val sampleList = ListBuffer[ArtistSample]()
          items.foreach(item => {
            sampleList.append(new ArtistSample(artist, (index + 1).toInt, item._2, item._1,
              ArtistModelUtils.extractFeatures(item._1)))
          })
          artist -> sampleList
      }

    val artistWithSampleSplits = artistWithSamples.randomSplit(Array(0.9, 0.1))

    val trainSamples = artistWithSampleSplits(0).flatMap(_._2).sortBy(_.getQid, ascending = true, 1)
    val testSamples = artistWithSampleSplits(1).flatMap(_._2).sortBy(_.getQid, ascending = true, 1)
    (trainSamples, testSamples)
  }

  private def addDateTimeToDir(baseDir: String, minusDay: Int): String = {
    val dateTime = new DateTime()
    baseDir + "/date=" + dateTime.minusDays(minusDay).toString("yyyyMMdd")
  }
}
