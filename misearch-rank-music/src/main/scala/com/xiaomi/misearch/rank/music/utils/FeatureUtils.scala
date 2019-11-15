package com.xiaomi.misearch.rank.music.utils

import com.xiaomi.misearch.rank.music.common.model.{Feature, MusicItem}
import com.xiaomi.misearch.rank.music.common.utils.MusicFeatureUtils
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConverters._
import scala.collection.mutable

object FeatureUtils {

  val ARTIST_VECTOR_LENGTH = 32

  val META_RANK = "meta_rank"
  val RES_RANK = "res_rank"
  val QUALITY = "quality"
  val QQ_SONG_RAW_RANK = "qq_song_raw_rank"
  val QQ_ARTIST_RAW_RANK = "qq_artist_raw_rank"
  val QQ_RANK = "qq_rank"
  val HAS_LYRIC = "has_lyric"

  val SONG_SEARCH_PLAY_COUNT = "song_search_play_count"
  val SONG_ARTIST_SEARCH_PLAY_COUNT = "song_artist_search_play_count"
  val SONG_ARTIST_SEARCH_FINISH_RATE = "song_artist_search_finish_rate"
  val SONG_ARTIST_SEARCH_COUNT = "song_artist_search_count"

  val ARTIST_SEARCH_COUNT = "artist_search_count"
  val ARTIST_MUSIC_COUNT = "artist_music_count"
  val ARTIST_ORIGIN_COUNT = "artist_origin_count"

  val ARTIST_VECTOR_ = "artist_vector_"
  val COVER_ARTIST_VECTOR_ = "cover_artist_vector_"

  val TAGS_ONE_HOT_ = "tag_"

  def convertToMusicItem(id: String, indexFeature: IndexFeature, statsFeature: StatsFeature, artistFeature: ArtistFeature,
                           coverArtistVector: Array[Double]): MusicItem = {
    val musicFeature = new MusicItem
    musicFeature.setId(id)
    musicFeature.setMetaRank(indexFeature.metaRank)
    musicFeature.setResRank(indexFeature.resRank)
    musicFeature.setQuality(indexFeature.quality)
    musicFeature.setQqSongRawRank(if (indexFeature.qqSongRawRank == 0) 100D else
      indexFeature.qqSongRawRank.toDouble)
    val qqArtistRawRank = getArtistRawRank(indexFeature.qqMultiArtistRawRank)
    musicFeature.setQqArtistRawRank(if (qqArtistRawRank == 0) 100D else qqArtistRawRank)
    musicFeature.setQqRank(indexFeature.qqRank)
    musicFeature.setHasLyric(if (indexFeature.hasLyric) 1D else 0D)
    musicFeature.setSongSearchPlayCount(statsFeature.songSearchPlayCount)
    musicFeature.setSongArtistSearchPlayCount(statsFeature.songArtistSearchPlayCount)
    musicFeature.setSongArtistSearchFinishRate(if (statsFeature.songArtistSearchPlayCount > 0)
        statsFeature.songArtistSearchFinishCount.toDouble / statsFeature.songArtistSearchPlayCount else 0D)
    musicFeature.setSongArtistSearchCount(statsFeature.songArtistSearchCount)
    musicFeature.setArtistSearchCount(artistFeature.artistSearchCount)
    musicFeature.setArtistMusicCount(artistFeature.artistMusicCount)
    musicFeature.setArtistOriginCount(artistFeature.artistOriginCount)
    if (artistFeature.artistVector != null) {
      musicFeature.setArtistVector(artistFeature.artistVector.toList.map(_.asInstanceOf[java.lang.Double]).asJava)
    }
    if (coverArtistVector != null) {
      musicFeature.setCoverArtistVector(coverArtistVector.toList.map(_.asInstanceOf[java.lang.Double]).asJava)
    }
    if (indexFeature.tags != null) {
      musicFeature.setTags(indexFeature.tags.toList.asJava)
    }
    musicFeature
  }

  def convertRawToFeatures(id: String, indexFeature: IndexFeature, statsFeature: StatsFeature, artistFeature: ArtistFeature,
                           coverArtistVector: Array[Double],
                           tagIndexMap: Broadcast[scala.collection.immutable.Map[String, Int]]) = {
    val musicItem = convertToMusicItem(id, indexFeature, statsFeature, artistFeature, coverArtistVector)
    MusicFeatureUtils.extractFeatures(musicItem,
      tagIndexMap.value.map{case (t, i) => t -> i.asInstanceOf[Integer]}.asJava).asScala
  }

  def convertToText(id: String, features: mutable.Buffer[Feature]): String = {
    val featureTextList = features
      .map(musicFeature => musicFeature.getId + ":" + musicFeature.getValue.formatted("%.3f"))
    id + " " + featureTextList.mkString(" ")
  }

  private def getArtistRawRank(multiRawRank: String): Int = {
    if (multiRawRank == null) {
      return 0
    }
    try {
      val ranks = multiRawRank.split(";")
      var rank = 0
      ranks.foreach(r => {
        val infos = r.split(":")
        rank += infos(1).toInt
      })
      return rank
    } catch {
      case _: Exception => return 0
    }
    0
  }

}
