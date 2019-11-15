package com.xiaomi.misearch.rank.music.prepare

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.data.spec.platform.misearch.SoundboxMusicSearchLog
import com.xiaomi.misearch.rank.music.model.MusicStat
import com.xiaomi.misearch.rank.music.utils.LogUtils._
import com.xiaomi.misearch.rank.music.utils.Paths
import com.xiaomi.misearch.rank.utils.PathUtils._
import com.xiaomi.misearch.rank.utils.SerializationUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

/**
  * @author Shenglan Wang
  */
object LabelGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val start = semanticDate(args.getOrElse("start", "-7"))
    val end = semanticDate(args.getOrElse("end", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var musicSearchLog = intermediateDateIntervalPath(Paths.SOUNDBOX_MUSIC_SEARCH_LOG, start, end)
    var qqRankPath = Paths.QQ_RANK_PATH

    var musicStatOutputPath = intermediateDatePath(Paths.MUSIC_STAT_PATH, end.toInt)
    var labelOutputPath = intermediateDatePath(Paths.LABEL_PATH, end.toInt)
    var queryOutputPath = Paths.QUERY_PATH

    var conf = new SparkConf()
      .setAppName(LabelGenerator.getClass.getName)

    if (dev) {
      musicSearchLog = Seq(Paths.SOUNDBOX_MUSIC_SEARCH_LOG_LOCAL)
      qqRankPath = Paths.QQ_RANK_PATH_LOCAL

      musicStatOutputPath = Paths.MUSIC_STAT_PATH_LOCAL
      labelOutputPath = Paths.LABEL_PATH_LOCAL
      queryOutputPath = Paths.QUERY_PATH_LOCAL
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    val qqRankMap = getQQRankMap(qqRankPath, spark)
    val musicStats = analysisLog(sc, musicSearchLog, qqRankMap)
    println("Music Stats line: " + musicStats.count())

    val labelData = musicStats.map(x => label(x)).filter(_._3 > 0)
    println("Label Data count: " + labelData.count())

    // Music Stats
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(musicStatOutputPath), true)
    musicStats.map(x => SerializationUtils.toJson(x)).repartition(1).saveAsTextFile(musicStatOutputPath)

    fs.delete(new Path(queryOutputPath), true)
    musicStats.map(x => x.getSong + "\t" + (if(StringUtils.isEmpty(x.getDisplaySong)) "" else x.getDisplaySong)).distinct().repartition(1).saveAsTextFile(queryOutputPath)

    // Label Data
    fs.delete(new Path(labelOutputPath), true)
    labelData.map(_.productIterator.mkString("\t")).repartition(1).saveAsTextFile(labelOutputPath)

    spark.stop()
    println("Job done!")
  }

  def label(musicData: MusicStat) = {
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
      if (finishRatio >= 0.6 || validListenRatio >= 0.8) {
        label = 5
      } else if ((finishRatio >= 0.4 && finishRatio < 0.6) && (validListenRatio >= 0.6 && validListenRatio < 0.8)) {
        label = 4
      } else {
        label = 1
      }
    } else {
      if (finishRatio >= 0.6 || validListenRatio >= 0.8) {
        label = 3
      } else if ((finishRatio >= 0.4 && finishRatio < 0.6) && (validListenRatio >= 0.6 && validListenRatio < 0.8)) {
        label = 2
      } else {
        label = 1
      }
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

    if (StringUtils.isNotBlank(markInfo)) {
      val splits = markInfo.split("::")
      if (splits.length > 0) {
        val items = splits(0).split("_")
        if (items.length == 2) {
          cp = items(0)
          musicId = items(1)
        }
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

  def analysisLog(sc: SparkContext, musicSearchLog: Seq[String], qqRankMap: Broadcast[scala.collection.Map[String, Array[String]]]) = {
    val invalidRankIdx = 1000
    sc.union(musicSearchLog.map { p =>
      sc.thriftParquetFile(p, classOf[SoundboxMusicSearchLog])
        .filter(_.song != null)
        .filter(_.musicid != null)
        .filter(_.cp != null)
        .map(log => extractMusicId(log) -> log)
        .filter(_._1.nonEmpty)
        .map {
          case (resourceId, line) =>
            var isFinished = 0
            val switchType = line.switchtype
            if (StringUtils.isNotBlank(switchType) && switchType.equals("autoswitch")) {
              isFinished = 1
            }
            val startTime = line.starttime
            val endTime = line.endtime
            val listenTime: Long = endTime - startTime
            var isValidListened = 0
            if (listenTime >= 30) {
              isValidListened = 1
            }

            (line.song, resourceId, isFinished, isValidListened, line.displaysong)
        }
    })
      .filter(x => StringUtils.isNotBlank(x._1))
      .filter(x => StringUtils.isNotBlank(x._2))
      .map { case (song, resourceId, isFinished, isValidListened, displaySong) => (song, resourceId, displaySong) -> (isFinished, isValidListened, 1) }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .filter(_._2._3 >= 50)
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

          if (rank == 0) {
            rank = invalidRankIdx
          }
          new MusicStat(song, resourceId, finishCount, playCount, validListenCount, displaySong, rank)
        }
      }
  }
}
