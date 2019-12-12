package com.xiaomi.misearch.rank.music.prepare

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.data.spec.log.tv.{MaterialMusicMid, TableName}
import com.xiaomi.data.spec.platform.misearch.SoundboxMusicSearchLog
import com.xiaomi.misearch.rank.music.model.MusicStat
import com.xiaomi.misearch.rank.music.utils.LogUtils._
import com.xiaomi.misearch.rank.music.utils.{Paths, QQRankUtils}
import com.xiaomi.misearch.rank.utils.PathUtils._
import com.xiaomi.misearch.rank.utils.SerializationUtils
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.util.control.Breaks

/**
  * @author Shenglan Wang
  */
object LabelGenerator {

  val invalidRankIdx = 1000

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val start = semanticDate(args.getOrElse("start", "-7"))
    val end = semanticDate(args.getOrElse("end", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var musicSearchLog = intermediateDateIntervalPath(Paths.SOUNDBOX_MUSIC_SEARCH_LOG, start, end)
    var qqRankPath = Paths.QQ_RANK_PATH
    var musicIdxPath = intermediateDatePath(Paths.MATERIAL_MUSIC_INDEX_DATA, end.toInt)

    var musicStatOutputPath = intermediateDatePath(Paths.MUSIC_STAT_PATH, end.toInt)
    var labelOutputPath = intermediateDatePath(Paths.LABEL_PATH, end.toInt)
    var queryOutputPath = Paths.QUERY_PATH

    var conf = new SparkConf()
      .setAppName(LabelGenerator.getClass.getName)

    if (dev) {
      musicSearchLog = Seq(Paths.SOUNDBOX_MUSIC_SEARCH_LOG_LOCAL)
      qqRankPath = Paths.QQ_RANK_PATH_LOCAL
      musicIdxPath = Paths.MATERIAL_MUSIC_INDEX_DATA_LOCAL

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

    val musicStats = analysisLog(sc, musicSearchLog)

    val qqRankMap = getQQRankMap(qqRankPath, spark)
    val musicIdMap = getMusicIdMap(musicIdxPath, sc, musicStats)
    val musicStatsFinal = musicStats.map { x =>
      val qqRank = calcQQRank(x.getResourceId, x.getSong, x.getDisplaySong, qqRankMap, musicIdMap)
      x.setQqRank(qqRank)
      x
    }
    println("Music Stats line: " + musicStatsFinal.count())

    val labelData = musicStatsFinal.map(x => label(x)).filter(_._3 > 0)
    println("Label Data count: " + labelData.count())

    // Music Stats
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(musicStatOutputPath), true)
    musicStatsFinal.map(x => SerializationUtils.toJson(x)).repartition(1).saveAsTextFile(musicStatOutputPath)

    fs.delete(new Path(queryOutputPath), true)
    musicStatsFinal.map(x => x.getSong + "\t" + (if (StringUtils.isEmpty(x.getDisplaySong)) "" else x.getDisplaySong)).distinct().repartition(1).saveAsTextFile(queryOutputPath)

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
    if (qqRank == 1) {
      label = 5
    } else if (playCount >= 50) {
      if (finishRatio >= 0.6 || validListenRatio >= 0.8) {
        label = 4
      } else if ((finishRatio >= 0.4 && finishRatio < 0.6) && (validListenRatio >= 0.6 && validListenRatio < 0.8)) {
        label = 3
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

  def getQQRankMap(qqRankPath: String, spark: SparkSession) = {
    spark.sparkContext.broadcast(spark.sparkContext.textFile(qqRankPath).map(x => {
      val items = x.split("\t")
      items(0) -> items(1).split("@")
    }).collectAsMap())
  }

  def getMusicIdMap(musicIdxPath: String, sc: SparkContext, musicStats: RDD[MusicStat]) = {
    val musicIdsExists = sc.broadcast(musicStats.map(x => x.getResourceId -> 1).collectAsMap())
    sc.broadcast(sc.thriftParquetFile(musicIdxPath, classOf[MaterialMusicMid])
      .filter(_.tableName == TableName.MUSIC)
      .map(_.musicMetaMid)
      .filter(x => CollectionUtils.isNotEmpty(x.resources))
      .flatMap {
        metaMid => {
          for (res <- metaMid.resources.asScala) yield {
            val singerNames = StringUtils.split(res.originSinger, ";").toList.asJava
            res.cpSongId -> QQRankUtils.createMusicBasicInfo(res.cpSongId, res.originAlbumName, singerNames)
          }
        }
      }.filter(x => musicIdsExists.value.contains(x._1)).collectAsMap())
  }

  def parseMusicBasicInfo(musicBasicInfo: String) = {
    val splits = musicBasicInfo.split("#")
    if (splits.length == 3) {
      val resId = splits(0)
      val album = splits(1)
      val singer = splits(2)
      (resId, album, singer)
    } else {
      ("", "", "")
    }
  }

  def analysisLog(sc: SparkContext, musicSearchLog: Seq[String]) = {
    sc.union(musicSearchLog.map { p =>
      sc.thriftParquetFile(p, classOf[SoundboxMusicSearchLog])
        .filter(_.song != null)
        .filter(_.musicid != null)
        .filter(_.cp != null)
        .filter(log => (isMatchField(log.song, log.songname) || isMatchField(log.song, log.songalias)))
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
      .filter(_._2._3 >= 20)
      .map {
        case ((song, resId, displaySong), (finishCount, validListenCount, playCount)) => {
          new MusicStat(song, resId, finishCount, playCount, validListenCount, displaySong, invalidRankIdx)
        }
      }
  }

  def calcQQRank(resId: String, song: String, displaySong: String, qqRankMap: Broadcast[scala.collection.Map[String, Array[String]]], musicIdMap: Broadcast[scala.collection.Map[String, String]]) = {
    var query = song
    if (StringUtils.isNotBlank(displaySong)) {
      query = displaySong
    }

    val musicBasicInfo = musicIdMap.value.getOrElse(resId, "")
    val tuple = parseMusicBasicInfo(musicBasicInfo)
    val album = tuple._2
    val singer = tuple._3

    val musicBasicInfoArr = qqRankMap.value.getOrElse(query, Array())
    var rank = 0
    val loop = new Breaks;
    loop.breakable {
      for (item <- musicBasicInfoArr) {
        rank += 1
        val compareToTuple = parseMusicBasicInfo(item)
        val compareToResId = compareToTuple._1
        val compareToAlbum = compareToTuple._2
        val compareToSinger = compareToTuple._3
        if (resId.equals(compareToResId)) {
          loop.break()
        } else if ((StringUtils.isNoneBlank(compareToAlbum) || StringUtils.isNoneBlank(compareToSinger)) && (compareToAlbum.equals(album) && compareToSinger.equals(singer))) {
          loop.break()
        }
      }
    }

    if (rank == 0) {
      rank = invalidRankIdx
    }
    rank
  }

}
