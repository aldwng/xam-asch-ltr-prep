package net.xam.ltr.prep.run.prepare

import xmd.commons.spark.HdfsIO._
import net.xam.asch.ltr.common.model.{ArtistItem, StoredMusicItem}
import net.xam.asch.ltr.utils.SerializationUtils
import net.xam.ltr.prep.run.utils.ArtistItemUtils._
import net.xam.ltr.prep.run.model.IndexFeature._
import net.xam.ltr.prep.run.utils.LogUtils._
import net.xam.ltr.prep.run.utils.MusicItemUtils._
import net.xam.ltr.prep.run.utils.Paths._
import net.xam.ltr.prep.run.model.{IndexFeature, StatsFeature}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object FeatureGenerator {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    val sparkContext = new SparkContext(sparkConf)

    val dataWithStats = generateDataWithStats(SOUNDBOX_MUSIC_SEARCH_LOG, 1, 15, sparkContext)
    val dataMap = sparkContext.broadcast(dataWithStats.collectAsMap())

    val dataWithFeatures = generateFeatureFromIndex(MATERIAL_MUSIC_INDEX_DATA, dataMap, sparkContext)

    val artistItems = generateArtistItem(SOUNDBOX_MUSIC_SEARCH_LOG, MATERIAL_MUSIC_INDEX_DATA, sparkContext)
    val artistItemMap = sparkContext.broadcast(artistItems.collectAsMap())

    val songArtistStatsMap = sparkContext.broadcast(readSongArtistStats(SOUNDBOX_SONG_ARTIST_PAIR, sparkContext))

    val rawFeatures = dataWithFeatures
      .map {
        case (id, (indexFeature, (pcount1, fcount1, pcount2, fcount2))) =>
          val artistFeatureList = ListBuffer[ArtistItem]()
          if (indexFeature.artists != null) {
            indexFeature.artists.foreach(artist => {
              artistFeatureList.append(artistItemMap.value.getOrElse(artist, null))
            })
          }
          val statsFeature = StatsFeature(getSongArtistSearchCount(indexFeature.song, indexFeature.artists, songArtistStatsMap),
            pcount1, fcount1, pcount2, fcount2)

          id -> (indexFeature, statsFeature, generateArtistFeatureFromMulti(artistFeatureList.filter(_ != null)))
      }
      .filter(_._2._3 != null)
      .filter(_._2._1 != null)

    val musicItems = rawFeatures
      .map {
        case (id, (indexFeature, statsFeature, artistItem)) =>
          val item = convertToMusicItem(id, indexFeature, statsFeature, artistItem)
          id -> item
      }

    val storedArtistFeatures = artistItems.filter(_._2.getSearchCount > 0)
      .map {
        case (_, artistFeature) =>
          SerializationUtils.toJson(artistFeature)
      }

    val storedFeatures = musicItems
      .map {
        case (_, musicItem) =>
          SerializationUtils.toJson(new StoredMusicItem(musicItem))
      }

    val features = musicItems
      .map {
        case (_, musicFeature) => SerializationUtils.toJson(musicFeature)
      }

    val hadoopConf = new Configuration(sparkContext.hadoopConfiguration)
    val fileSystem: FileSystem = FileSystem.get(hadoopConf)

    fileSystem.delete(new Path(SOUNDBOX_ARTIST_STORED_FEATURE), true)
    storedArtistFeatures.repartition(1).saveAsTextFile(SOUNDBOX_ARTIST_STORED_FEATURE)

    fileSystem.delete(new Path(SOUNDBOX_MUSIC_STORED_FEATURE), true)
    storedFeatures.repartition(1).saveAsTextFile(SOUNDBOX_MUSIC_STORED_FEATURE)

    fileSystem.delete(new Path(SOUNDBOX_MUSIC_FEATURE), true)
    features.repartition(1).saveAsTextFile(SOUNDBOX_MUSIC_FEATURE)

    sparkContext.stop()
  }

  private def readSongArtistStats(path: String, sc: SparkContext) = {
    sc.textFile(path)
      .map {
        line => {
          val args = line.trim.split("\t")
          (args(0), args(1)) -> args(2).toLong
        }
      }
      .collectAsMap()
  }

  private def getSongArtistSearchCount(song: String, artistList: Array[String],
                                       songArtistMap: Broadcast[scala.collection.Map[(String, String), Long]]): Long = {
    if (artistList == null || artistList.isEmpty) {
      return 0L
    }
    if (artistList.length == 1) {
      return songArtistMap.value.getOrElse((song, artistList.head), 0L)
    }
    var count = 0L
    val artists = artistList.sortWith((o1, o2) => o1 < o2).mkString(";")
    count += songArtistMap.value.getOrElse((song, artists.sortWith((s1, s2) => s1 < s2).mkString(";")), 0L)
    artistList.foreach(a => {
      count += songArtistMap.value.getOrElse((song, a), 0L)
    })
    count = count / (artists.length + 1)
    count
  }

  private def generateDataWithStats(path: String, recentDay: Int, timeSpan: Int, sc: SparkContext) = {
    val dateTime = new DateTime()
    sc.union(Range(recentDay, recentDay + timeSpan).map {
      day => {
        val inputPath = path + "/date=" + dateTime.minusDays(day).toString("yyyyMMdd")
        sc.thriftParquetFile(inputPath, classOf[SoundboxMusicSearchLog])
          .filter(_.song != null)
          .filter(_.musicid != null)
          .filter(_.cp != null)
          .filter(_.found)
          .filter(log => (isMatchField(log.song, log.songname) || isMatchField(log.song, log.songalias)) &&
            isMatchField(log.artist, log.artistname))
          .map {
            log => extractMusicId(log) -> log
          }
          .filter(_._1.nonEmpty)
          .map {
            case (musicId, log) =>
              (musicId, log.artist == null, log.requestid) -> (if ("autoswitch" == log.switchtype) 1 else 0)
          }
          .reduceByKey(_ + _)
          .map {
            case ((id, isOnlySong, _), finishCount) =>
              (id, isOnlySong) -> (1L, if (finishCount > 0) 1L else 0L)
          }
          .reduceByKey((o1, o2) => (o1._1 + o2._1, o1._2 + o2._2))
      }
    })
      .reduceByKey((o1, o2) => (o1._1 + o2._1, o1._2 + o2._2))
      .map {
        case ((id, isOnlySong), (pcount, fcount)) =>
          id -> (if (isOnlySong) (pcount, fcount, 0L, 0L) else (0L, 0L, pcount, fcount))
      }
      .reduceByKey((i1, i2) => (i1._1 + i2._1, i1._2 + i2._2, i1._3 + i2._3, i1._4 + i2._4))
  }


  private def generateFeatureFromIndex(path: String,
                                       idMap: Broadcast[scala.collection.Map[String, (Long, Long, Long, Long)]],
                                       sc: SparkContext) = {
    val dateTime = new DateTime
    val indexPath = path + "/date=" + dateTime.minusDays(1).toString("yyyyMMdd")
    sc.thriftParquetFile(indexPath, classOf[MaterialMusicMid])
      .filter(_.tableName == TableName.MUSIC)
      .map(_.musicMetaMid)
      .filter(_.resources != null)
      .filter(!_.resources.isEmpty)
      .flatMap {
        metaMid => {
          val flatList = ListBuffer[(String, (IndexFeature, (Long, Long, Long, Long)))]()
          metaMid.resources.asScala.foreach(resMid => {
            if (idMap.value.contains(resMid.cpSongId)) {
              flatList.append(resMid.cpSongId -> (getFeatureFromIndexTable(metaMid, resMid),
                idMap.value.getOrElse(resMid.cpSongId, (0L, 0L, 0L, 0L))))
            }
          })
          flatList
        }
      }
  }

  private def generateArtistItem(logPath: String, idxPath: String, sc: SparkContext) = {
    val dateTime = new DateTime()
    val searchCountRdd = sc.union(Range(2, 16).map {
      day => {
        val inputPath = logPath + "/date=" + dateTime.minusDays(day).toString("yyyyMMdd")
        sc.thriftParquetFile(inputPath, classOf[SoundboxMusicSearchLog])
          .filter(_.artist != null)
          .map {
            log => {
              (log.artist, log.requestid) -> 1
            }
          }
          .reduceByKey(_ + _)
          .map(_._1._1 -> 1L)
          .reduceByKey(_ + _)
      }
    })
      .reduceByKey(_ + _)

    val idxInputPath = idxPath + "/date=" + dateTime.minusDays(1).toString("yyyyMMdd")
    val idxRdd = sc.thriftParquetFile(idxInputPath, classOf[MaterialMusicMid])
    val idxMusicCountRdd = idxRdd
      .filter(_.tableName == TableName.MUSIC)
      .map(_.musicMetaMid)
      .filter(_.resources != null)
      .flatMap {
        metaMid => {
          countArtistInfoFromIndex(metaMid)
        }
      }
      .filter(_ != null)
      .reduceByKey((a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))

    val idxArtistRdd = idxRdd
      .filter(_.tableName == TableName.ARTIST)
      .map(_.musicArtistMid)
      .map {
        artistMid => {
          artistMid.artistId -> artistMid.artistName
        }
      }

    val artistRdd = (idxArtistRdd leftOuterJoin idxMusicCountRdd)
      .map {
        case (id, (name, count)) =>
          name -> (id, count.getOrElse(0L, 0L)._1, count.getOrElse(0L, 0L)._2)
      }

    val artistFeatureRdd = (artistRdd leftOuterJoin searchCountRdd)
      .map {
        case (artistName, ((_, musicCount, originCount), searchCount)) =>
          artistName -> new ArtistItem(artistName, searchCount.getOrElse(0L), musicCount, originCount)
      }
    artistFeatureRdd
  }
}
