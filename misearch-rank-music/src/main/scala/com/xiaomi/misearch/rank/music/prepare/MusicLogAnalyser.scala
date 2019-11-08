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
object MusicLogAnalyser {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val start = semanticDate(args.getOrElse("start", "-7"))
    val end = semanticDate(args.getOrElse("end", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var musicSearchLog = intermediateDateIntervalPath(Paths.MUSIC_SEARCH_LOG, start, end)
    var musicDataPath = intermediateDatePath(Paths.MUSIC_DATA_PATH, end.toInt)
    var queryPath = intermediateDatePath(Paths.QUERY_PATH, end.toInt)
    var qqRankPath = Paths.QQ_RANK_PATH
    var conf = new SparkConf()
      .setAppName(MusicLogAnalyser.getClass.getName)

    if (dev) {
      musicSearchLog = Seq(Paths.MUSIC_SEARCH_LOG_LOCAL)
      musicDataPath = Paths.MUSIC_DATA_PATH_LOCAL
      queryPath = Paths.QUERY_PATH_LOCAL
      qqRankPath = Paths.QQ_RANK_PATH_LOCAL
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val qqRankMap = getQQRankMap(qqRankPath, spark)
    val data = analysis(spark, musicSearchLog, qqRankMap)

    println("lines: " + data.count())
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(musicDataPath), true)
    data.map(x => SerializationUtils.toJson(x)).repartition(1).saveAsTextFile(musicDataPath)

    fs.delete(new Path(queryPath), true)
    data.map(x => x.getSong + "\t" + x.getDisplaySong).distinct().repartition(1).saveAsTextFile(queryPath)
    spark.stop()
    println("Job done!")
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

  def analysis(spark: SparkSession, musicSearchLog: Seq[String], qqRankMap: Broadcast[scala.collection.Map[String, Array[String]]]) = {
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
          if(StringUtils.isNotBlank(displaySong)) {
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
