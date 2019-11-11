package com.xiaomi.misearch.rank.music.prepare

import com.twitter.scalding.Args
import com.xiaomi.misearch.rank.music.model.{MusicData, MusicSearchLog}
import com.xiaomi.misearch.rank.music.utils.Paths
import com.xiaomi.misearch.rank.utils.PathUtils._
import com.xiaomi.misearch.rank.utils.SerializationUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.util.control._

/**
  * @author Shenglan Wang
  */
object LabelGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val start = semanticDate(args.getOrElse("start", "-7"))
    val end = semanticDate(args.getOrElse("end", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var musicSearchLog = intermediateDateIntervalPath(Paths.MUSIC_SEARCH_LOG, start, end)
    var qqRankPath = Paths.QQ_RANK_PATH

    var musicDataOutputPath = intermediateDatePath(Paths.MUSIC_DATA_PATH, end.toInt)
    var labelOutputPath = intermediateDatePath(Paths.LABEL_PATH, end.toInt)
    var queryOutputPath = intermediateDatePath(Paths.QUERY_PATH, end.toInt)
    var zippedQueryOutputPath = intermediateDatePath(Paths.ZIPPED_QUERY_PATH, end.toInt)
    var conf = new SparkConf()
      .setAppName(LabelGenerator.getClass.getName)

    if (dev) {
      musicSearchLog = Seq(Paths.MUSIC_SEARCH_LOG_LOCAL)
      qqRankPath = Paths.QQ_RANK_PATH_LOCAL

      musicDataOutputPath = Paths.MUSIC_DATA_PATH_LOCAL
      labelOutputPath = Paths.LABEL_PATH_LOCAL
      queryOutputPath = Paths.QUERY_PATH_LOCAL
      zippedQueryOutputPath = Paths.ZIPPED_QUERY_PATH_LOCAL
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val qqRankMap = getQQRankMap(qqRankPath, spark)
    val musicData = analysisLog(spark, musicSearchLog, qqRankMap)
    println("Music Data count: " + musicData.count())

    val labelData = musicData.map(x => labelMusicData(x)).filter(_._3 > 0)
    println("Label Data count: " + labelData.count())

    // Music Data
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(musicDataOutputPath), true)
    musicData.map(x => SerializationUtils.toJson(x)).repartition(1).saveAsTextFile(musicDataOutputPath)

    fs.delete(new Path(queryOutputPath), true)
    musicData.map(x => x.getSong + "\t" + x.getDisplaySong).distinct().repartition(1).saveAsTextFile(queryOutputPath)

    // Label Data
    val queries = labelData.map(_._1).distinct()
    val zippedQueries = queries.zipWithIndex().map(x => x._1 -> (x._2 + 1))

    val queryDataSplits = zippedQueries.randomSplit(Array(0.9, 0.1))
    val train = spark.sparkContext.broadcast(queryDataSplits(0).collectAsMap())
    val test = spark.sparkContext.broadcast(queryDataSplits(1).collectAsMap())

    val trainOutputPath = labelOutputPath + "/train"
    val testOutputPath = labelOutputPath + "/test"
    fs.delete(new Path(trainOutputPath), true)
    fs.delete(new Path(testOutputPath), true)
    labelData.filter(x => train.value.contains(x._1)).map(_.productIterator.mkString("\t")).repartition(1).saveAsTextFile(trainOutputPath)
    labelData.filter(x => test.value.contains(x._1)).map(_.productIterator.mkString("\t")).repartition(1).saveAsTextFile(testOutputPath)

    fs.delete(new Path(zippedQueryOutputPath), true)
    spark.sparkContext.makeRDD(zippedQueries.sortBy(_._2).collect().map(_.productIterator.mkString("\t")), 1).repartition(1).saveAsTextFile(zippedQueryOutputPath)

    spark.stop()
    println("Job done!")
  }

  def labelMusicData(musicData: MusicData) = {
    val song = musicData.getSong
    val finishCount = musicData.getFinishCount
    val validListenCount = musicData.getValidListenCount
    val playCount = musicData.getPlayCount
    val qqRank = musicData.getQqRank()
    val resourceId = musicData.getResourceId()

    val finishRatio = 1.0f * finishCount / playCount
    val validListenRatio = 1.0f * validListenCount / playCount

    var label = 0
    if (qqRank <= 3) {
      label = 5
    } else if (playCount >= 100) {
      if (finishRatio >= 0.6 || validListenRatio >= 0.6) {
        label = 5
      } else if ((finishRatio >= 0.4 && finishRatio < 0.6) && (validListenRatio >= 0.4 && validListenRatio < 0.6)) {
        label = 4
      }
      label = 1
    } else {
      if (finishRatio >= 0.6 || validListenRatio >= 0.6) {
        label = 3
      } else if ((finishRatio >= 0.4 && finishRatio < 0.6) && (validListenRatio >= 0.4 && validListenRatio < 0.6)) {
        label = 2
      }
      label = 1
    }

    (song, resourceId, label)
  }

  /**
    * markInfo format: xiaowei_123456::xxx
    *
    * @param markInfo
    * @return
    */
  def parseMarkInfo(markInfo: String) = {
    var cp = ""
    var musicId = ""

    val splits = markInfo.split("::")
    if (splits.length > 0) {
      val items = splits(0).split("_")
      if (items.length == 2) {
        cp = items(0)
        musicId = items(1)
      }
    }
    (cp, musicId)
  }

  def getQQRankMap(qqRankPath: String, spark: SparkSession) = {
    spark.sparkContext.broadcast(spark.sparkContext.textFile(qqRankPath).map(x => {
      val items = x.split("\t")
      items(0) -> items(1).split(",")
    }).collectAsMap())
  }

  def analysisLog(spark: SparkSession, musicSearchLog: Seq[String], qqRankMap: Broadcast[scala.collection.Map[String, Array[String]]]) = {
    import spark.implicits._
    spark.read
      .parquet(musicSearchLog: _*)
      .as[MusicSearchLog]
      .rdd
      .map { x =>
        val song = x.song.getOrElse("")
        var musicId = x.musicid.getOrElse("")
        val displaySong = x.displaysong.getOrElse("")
        var cp = x.cp.getOrElse("")

        if (musicId.startsWith("mv")) {
          val values: (String, String) = parseMarkInfo(x.markinfo.getOrElse(""))
          cp = values._1
          musicId = values._2
        }

        var isFinished = 0
        val switchType = x.switchType.getOrElse("")
        if (StringUtils.isNotBlank(switchType) && switchType.equals("autoswitch")) {
          isFinished = 1
        }
        val startTime = x.starttime.getOrElse(0L)
        val endTime = x.endTime.getOrElse(0L)
        val listenTime: Long = endTime - startTime
        var isValidListened = 0
        if (listenTime >= 30) {
          isValidListened = 1
        }

        (song, cp, musicId, isFinished, isValidListened, displaySong)
      }
      .filter(x => StringUtils.isNotBlank(x._1))
      .filter(x => StringUtils.isNotBlank(x._2))
      .filter(x => StringUtils.isNotBlank(x._3))
      .map { case (song, cp, musicId, isFinished, isValidListened, displaySong) => (song, cp + "_" + musicId, displaySong) -> (isFinished, isValidListened, 1) }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .filter(_._2._3 >= 10)
      .map {
        case ((song, resourceId, displaySong), (finishCount, validListenCount, playCount)) => {
          var query = song
          if (StringUtils.isNotBlank(displaySong)) {
            query = displaySong
          }
          val musicIds = qqRankMap.value.getOrElse(query, Array())
          var rank = 0
          if (musicIds.contains(resourceId)) {
            val loop = new Breaks;
            loop.breakable {
              for (musicId <- musicIds) {
                rank += 1
                if (resourceId.equals(musicId)) {
                  loop.break()
                }
              }
            }
          }
          new MusicData(song, resourceId, finishCount, playCount, validListenCount, displaySong, rank)
        }
      }
  }
}
