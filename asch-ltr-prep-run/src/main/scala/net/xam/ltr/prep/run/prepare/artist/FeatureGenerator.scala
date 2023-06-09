package net.xam.ltr.prep.run.prepare.artist

import com.google.gson.JsonObject
import net.xam.asch.ltr.common.model.artist.{ArtistMusicItem, ArtistStoredQueryItem}
import net.xam.asch.ltr.utils.GsonUtils.{getIntProp, getJsonObject, getStrProp, isNotEmpty}
import net.xam.ltr.prep.run.utils.AiContentLogUtils.{getCertainMusic, getMusicArray, isEffectiveMusicLog}
import net.xam.ltr.prep.run.utils.LogConstants._
import net.xam.ltr.prep.run.utils.LogUtils._
import net.xam.ltr.prep.run.utils.Paths._
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

object FeatureGenerator {

  def generateMusicStatsFeature(sc: SparkContext): RDD[(String, ArtistMusicItem)] = {
    val dateTime = new DateTime()
    sc.union(Range(1, 14).map {
      i => {
        val logPath = SOUNDBOX_MUSIC_SEARCH_LOG + "/date=" + dateTime.minusDays(i).toString("yyyyMMdd")
        val singleLogQueryStats = sc.thriftParquetFile(logPath, classOf[SoundboxMusicSearchLog])
          .filter(log => isValidMusicStatsScenario(log))
          .map {
            log => {
              val tupleStats = getSingleRequestMusicStats(log)
              (extractMusicId(log), log.requestid) -> (tupleStats._1._1, tupleStats._1._2, tupleStats._1._3,
                tupleStats._2._1, tupleStats._2._2, tupleStats._2._3, tupleStats._3._1, tupleStats._3._2,
                tupleStats._3._3)
            }
          }
          .reduceByKey((o1, o2) => (o1._1 + o2._1, o1._2 + o2._2, o1._3 + o2._3, o1._4 + o2._4, o1._5 + o2._5,
            o1._6 + o2._6, o1._7 + o2._7, o1._8 + o2._8, o1._9 + o2._9))
          .map {
            case ((id, _), (c11, c12, c13, c21, c22, c23, c31, c32, c33)) =>
              id -> (getCountByCondition(c11), getCountByCondition(c12), getCountByCondition(c13),
                getCountByCondition(c21), getCountByCondition(c22), getCountByCondition(c23),
                getCountByCondition(c31), getCountByCondition(c32), getCountByCondition(c33))
          }
          .reduceByKey((o1, o2) => (o1._1 + o2._1, o1._2 + o2._2, o1._3 + o2._3, o1._4 + o2._4, o1._5 + o2._5,
            o1._6 + o2._6, o1._7 + o2._7, o1._8 + o2._8, o1._9 + o2._9))
        singleLogQueryStats
      }
    })
      .reduceByKey((o1, o2) => (o1._1 + o2._1, o1._2 + o2._2, o1._3 + o2._3, o1._4 + o2._4, o1._5 + o2._5,
        o1._6 + o2._6, o1._7 + o2._7, o1._8 + o2._8, o1._9 + o2._9))
      .filter(e => e._2._1 > 10 || e._2._4 > 10 || e._2._7 > 10)
      .map {
        case (id, (c11, c12, c13, c21, c22, c23, c31, c32, c33)) =>
          id -> new ArtistMusicItem(id, c11, c12, c13, c21, c22, c23, c31, c32, c33)
      }
  }

  def genStatsFeatures(sc: SparkContext): RDD[(String, ArtistMusicItem)] = {
    val dateTime = new DateTime()
    sc.union(Range(1, 14).map {
      i => {
        val logPath = AI_CONTENT_FRONT_BACK_LOG + "/date=" + dateTime.minusDays(i).toString("yyyyMMdd")
        val singleLogQueryStats = sc.thriftParquetFile(logPath, classOf[AiContentFrontBackLog])
          .filter(log => isEffectiveMusicLog(log))
          .flatMap {
            log => {
              val iObj = getJsonObject(log.getIntention)
              val cObj = getJsonObject(log.getContent)
              val musicArr = getMusicArray(cObj)
              val fl = new ListBuffer[((String, String), (Int, Int, Int, Int, Int, Int, Int, Int, Int))]
              if (CollectionUtils.isNotEmpty(log.getPlayInfoLog) && isNotEmpty(musicArr)) {
                log.getPlayInfoLog.asScala.foreach(pil => {
                  val musicObj = getCertainMusic(musicArr, pil.getResid)
                  if (isValidForMusicStats(iObj, cObj, musicObj, pil)) {
                    val tupleStats = getSingleRequestMusicStats(pil, iObj)
                    fl.append((extractMusicId(pil, musicObj), log.getRequestid) ->
                      (tupleStats._1._1, tupleStats._1._2, tupleStats._1._3, tupleStats._2._1, tupleStats._2._2,
                        tupleStats._2._3, tupleStats._3._1, tupleStats._3._2, tupleStats._3._3))
                  }
                })
              }
              fl
            }
          }
          .reduceByKey((o1, o2) => (o1._1 + o2._1, o1._2 + o2._2, o1._3 + o2._3, o1._4 + o2._4, o1._5 + o2._5,
            o1._6 + o2._6, o1._7 + o2._7, o1._8 + o2._8, o1._9 + o2._9))
          .map {
            case ((id, _), (c11, c12, c13, c21, c22, c23, c31, c32, c33)) =>
              id -> (getCountByCondition(c11), getCountByCondition(c12), getCountByCondition(c13),
                getCountByCondition(c21), getCountByCondition(c22), getCountByCondition(c23),
                getCountByCondition(c31), getCountByCondition(c32), getCountByCondition(c33))
          }
          .reduceByKey((o1, o2) => (o1._1 + o2._1, o1._2 + o2._2, o1._3 + o2._3, o1._4 + o2._4, o1._5 + o2._5,
            o1._6 + o2._6, o1._7 + o2._7, o1._8 + o2._8, o1._9 + o2._9))
        singleLogQueryStats
      }
    })
      .reduceByKey((o1, o2) => (o1._1 + o2._1, o1._2 + o2._2, o1._3 + o2._3, o1._4 + o2._4, o1._5 + o2._5,
        o1._6 + o2._6, o1._7 + o2._7, o1._8 + o2._8, o1._9 + o2._9))
      .filter(e => e._2._1 > 10 || e._2._4 > 10 || e._2._7 > 10)
      .map {
        case (id, (c11, c12, c13, c21, c22, c23, c31, c32, c33)) =>
          id -> new ArtistMusicItem(id, c11, c12, c13, c21, c22, c23, c31, c32, c33)
      }
      .filter(x => x._2.getSongSearchPlayCount > 3 || x._2.getSongArtistSearchPlayCount > 3)
  }

  def generateQueryStatsFeature(sc: SparkContext): RDD[((String, String), Long)] = {
    val dateTime = new DateTime()
    sc.union(Range(1, 14).map {
      i => {
        val logPath = SOUNDBOX_MUSIC_SEARCH_LOG + "/date=" + dateTime.minusDays(i).toString("yyyyMMdd")
        val singleLogQueryStats = sc.thriftParquetFile(logPath, classOf[SoundboxMusicSearchLog])
          .filter(log => isValidQueryStatsScenario(log))
          .map {
            log => {
              (log.song, log.artist, log.requestid) -> 1
            }
          }
          .reduceByKey(_ + _)
          .flatMap {
            case ((song, artist, _), _) =>
              val qList = ListBuffer[((String, String), Long)]()
              if (artist != null && artist.contains("|")) {
                artist.split("\\|").foreach(singer => {
                  qList.append((song, singer) -> 1L)
                })
              } else {
                qList.append((song, artist) -> 1L)
              }
              qList
          }
          .reduceByKey(_ + _)
        singleLogQueryStats
      }
    })
      .reduceByKey(_ + _)
  }

  def genQueryStatsFeatures(sc: SparkContext): RDD[((String, String), Long)] = {
    val dateTime = new DateTime()
    sc.union(Range(1, 14).map {
      i => {
        val logPath = AI_CONTENT_FRONT_BACK_LOG + "/date=" + dateTime.minusDays(i).toString("yyyyMMdd")
        val singleLogQueryStats = sc.thriftParquetFile(logPath, classOf[AiContentFrontBackLog])
          .filter(log => isEffectiveMusicLog(log) && isValidForQueryStats(log))
          .map {
            log => {
              val iObj = getJsonObject(log.getIntention)
              val song = getStrProp(iObj, PROP_INTENTION_SONG)
              val artist = getStrProp(iObj, PROP_INTENTION_ARTIST)
              (song, artist, log.getRequestid) -> 1L
            }
          }
          .reduceByKey(_ + _)
          .flatMap {
            case ((song, artist, _), _) =>
              val qList = ListBuffer[((String, String), Long)]()
              if (artist != null && artist.contains("|")) {
                artist.split("\\|").foreach(singer => {
                  qList.append((song, singer) -> 1L)
                })
              } else {
                qList.append((song, artist) -> 1L)
              }
              qList
          }
          .reduceByKey(_ + _)
        singleLogQueryStats
      }
    })
      .reduceByKey(_ + _)
      .filter(_._2 > 3)
  }

  def getMusicDataInfo(sc: SparkContext, idSet: Broadcast[scala.collection.immutable.Set[String]]):
  RDD[(String, (String, String, Boolean))] = {
    val dateTime = new DateTime()
    val dataPath = MATERIAL_MUSIC_INDEX_DATA + "/date=" + dateTime.minusDays(2).toString("yyyyMMdd")
    sc.thriftParquetFile(dataPath, classOf[MaterialMusicMid])
      .filter(_.tableName == TableName.MUSIC)
      .map(_.musicMetaMid)
      .filter(_.resources != null)
      .filter(_.artistName != null)
      .filter(_.artistName.size() == 1)
      .flatMap {
        metaMid => {
          val infoList = ListBuffer[(String, (String, String, Boolean))]()
          metaMid.resources.asScala.foreach(r => {
            var isOrigin = false
            if (r.tags != null) {
              if (r.tags.asScala.map(_.name).toSet.contains("原唱")) {
                isOrigin = true
              }
            }
            if (idSet.value.contains(r.cpSongId)) {
              infoList.append(r.cpSongId -> (metaMid.name, metaMid.artistName.get(0), isOrigin))
            }
          })
          infoList
        }
      }
  }

  def addMusicInfoToMusicItems(musicItems: RDD[(String, ArtistMusicItem)], idInfoMap:
  Broadcast[scala.collection.Map[String, (String, String, Boolean)]]): RDD[(String, ArtistMusicItem)] = {
    musicItems
      .map {
        mi => {
          val musicItem = mi._2
          val musicInfo = idInfoMap.value.getOrElse(mi._1, (null, null, false))
          musicItem.setSongName(musicInfo._1)
          musicItem.setArtistName(musicInfo._2)
          musicItem.setOrigin(musicInfo._3)
          mi._1 -> musicItem
        }
      }
      .filter(_._2.getSongName != null)
      .filter(_._2.getArtistName != null)
  }

  def addQueryStatsToMusicItems(musicItems: RDD[(String, ArtistMusicItem)], idInfoMap:
  Broadcast[scala.collection.Map[(String, String), Long]]): RDD[(String, ArtistMusicItem)] = {
    musicItems
      .map {
        mi => {
          val musicItem = mi._2
          val songSearchCount = idInfoMap.value.getOrElse((musicItem.getSongName, null), 0L)
          val songArtistSearchCount = idInfoMap.value.getOrElse((musicItem.getSongName, musicItem.getArtistName), 0L)
          musicItem.setSongSearchCount(songSearchCount)
          musicItem.setSongArtistSearchCount(songArtistSearchCount)
          mi._1 -> musicItem
        }
      }
      .filter(_._2.getSongName != null)
      .filter(_._2.getArtistName != null)
  }

  def transformQueryStatsRddToStorable(queryStats: RDD[((String, String), Long)]): RDD[ArtistStoredQueryItem] = {
    queryStats
      .map {
        case ((song, artist), count) =>
          new ArtistStoredQueryItem(song, artist, count)
      }
  }

  def transformToArtistWithMusicItems(musicStats: RDD[(String, ArtistMusicItem)]): RDD[(String, List[ArtistMusicItem])] = {
    musicStats
      .map {
        case (_, musicItem) =>
          musicItem.getArtistName -> List(musicItem)
      }
      .reduceByKey(_ ::: _)
      .filter(_._2.length > 2)
      .filter {
        case (_, items) =>
          var isArtistSearchExposed = false
          items.foreach(item => {
            isArtistSearchExposed = isArtistSearchExposed || item.isExposedArtistSearch
          })
          isArtistSearchExposed
      }
  }

  private def isValidQueryStatsScenario(log: SoundboxMusicSearchLog): Boolean = {
    log.found &&
      (log.song != null && log.album == null && log.tag == null && (log.artist == null || !log.artist.contains(";")))
  }

  private def isValidMusicStatsScenario(log: SoundboxMusicSearchLog): Boolean = {
    val isValidQuery = log.found &&
      log.album == null && log.tag == null && (log.song != null || (log.artist != null && !log.artist.contains(";")))
    if (!isValidQuery) {
      return false
    }
    if (log.song != null && log.artist == null) {
      isMatchField(log.song, log.songname) || isMatchField(log.song, log.songalias)
    } else if (log.song != null && log.artist != null) {
      log.offset == 0
    } else {
      true
    }
  }

  private def isValidForQueryStats(log: AiContentFrontBackLog): Boolean = {
    val cObj = getJsonObject(log.getContent)
    val iObj = getJsonObject(log.getIntention)
    if (cObj == null || iObj == null) {
      return false
    }
    val isFound = getIntProp(cObj, PROP_CONTENT_IS_FOUND) == 1
    val song = getStrProp(iObj, PROP_INTENTION_SONG)
    val album = getStrProp(iObj, PROP_INTENTION_ALBUM)
    val tag = getStrProp(iObj, PROP_INTENTION_TAG)
    val artist = getStrProp(iObj, PROP_INTENTION_ARTIST)
    isFound && (StringUtils.isNotBlank(song) && StringUtils.isBlank(album) && StringUtils.isBlank(tag) &&
      (StringUtils.isBlank(artist) || !artist.contains(";")))
  }

  private def isValidForMusicStats(iObj: JsonObject, cObj: JsonObject, musicObj: JsonObject, piLog: PlayInfoLog):
  Boolean = {
    if (musicObj == null || piLog == null || iObj == null || cObj == null) {
      return false
    }
    val isFound = getIntProp(cObj, PROP_CONTENT_IS_FOUND) == 1
    val song = getStrProp(iObj, PROP_INTENTION_SONG)
    val album = getStrProp(iObj, PROP_INTENTION_ALBUM)
    val tag = getStrProp(iObj, PROP_INTENTION_TAG)
    val artist = getStrProp(iObj, PROP_INTENTION_ARTIST)

    val isValidQuery = isFound &&
      StringUtils.isBlank(album) && StringUtils.isBlank(tag) && (StringUtils.isNotBlank(song) ||
      (StringUtils.isNotBlank(artist) && !artist.contains(";")))
    if (!isValidQuery) {
      return false
    }
    val songName = getStrProp(musicObj, "song")
    val songAlias = getStrProp(musicObj, "songAlias")
    if (StringUtils.isNotBlank(song) && StringUtils.isBlank(artist)) {
      isMatchField(song, songName) || isMatchField(song, songAlias)
    } else if (StringUtils.isNotBlank(song) && StringUtils.isNotBlank(artist)) {
      piLog.getOffset == 0
    } else {
      true
    }
  }

  private def getSingleRequestMusicStats(log: SoundboxMusicSearchLog):
  ((Int, Int, Int), (Int, Int, Int), (Int, Int, Int)) = {
    val finishCount = if ("autoswitch" == log.switchtype) 1 else 0
    val finish30sCount = if (isFinish30s(log)) 1 else 0
    val requestStats = (1, finishCount, finish30sCount)
    val emptyStats = (0, 0, 0)
    if (log.song != null && log.artist == null) {
      (requestStats, emptyStats, emptyStats)
    } else if (log.song != null && log.artist != null) {
      (emptyStats, requestStats, emptyStats)
    } else {
      (emptyStats, emptyStats, requestStats)
    }
  }

  private def getSingleRequestMusicStats(piLog: PlayInfoLog, iObj: JsonObject):
  ((Int, Int, Int), (Int, Int, Int), (Int, Int, Int)) = {
    val finishCount = if ("autoswitch" == piLog.getSwitchtype) 1 else 0
    val finish30sCount = if (isFinish30s(piLog)) 1 else 0
    val requestStats = (1, finishCount, finish30sCount)
    val emptyStats = (0, 0, 0)
    val song = getStrProp(iObj, PROP_INTENTION_SONG)
    val artist = getStrProp(iObj, PROP_INTENTION_ARTIST)
    if (StringUtils.isNotBlank(song) && StringUtils.isBlank(artist)) {
      (requestStats, emptyStats, emptyStats)
    } else if (StringUtils.isNotBlank(song) && StringUtils.isNotBlank(artist)) {
      (emptyStats, requestStats, emptyStats)
    } else {
      (emptyStats, emptyStats, requestStats)
    }
  }

  private def getCountByCondition(count: Int): Long = {
    if (count > 0) {
      1L
    } else {
      0L
    }
  }
}
