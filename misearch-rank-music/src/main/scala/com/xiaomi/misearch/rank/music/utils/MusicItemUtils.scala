package com.xiaomi.misearch.rank.music.utils

import com.xiaomi.misearch.rank.music.common.model.MusicItem
import com.xiaomi.misearch.rank.music.model.{ArtistFeature, IndexFeature, StatsFeature}

import scala.collection.JavaConverters._

object MusicItemUtils {

  def convertToMusicItem(id: String, indexFeature: IndexFeature, statsFeature: StatsFeature, artistFeature: ArtistFeature,
                           coverArtistVector: Array[Double]): MusicItem = {
    val musicItem = new MusicItem
    musicItem.setId(id)
    musicItem.setMetaRank(indexFeature.metaRank)
    musicItem.setResRank(indexFeature.resRank)
    musicItem.setQuality(indexFeature.quality)
    musicItem.setQqSongRawRank(if (indexFeature.qqSongRawRank == 0) 100D else
      indexFeature.qqSongRawRank.toDouble)
    val qqArtistRawRank = getArtistRawRank(indexFeature.qqMultiArtistRawRank)
    musicItem.setQqArtistRawRank(if (qqArtistRawRank == 0) 100D else qqArtistRawRank)
    musicItem.setQqRank(indexFeature.qqRank)
    musicItem.setHasLyric(if (indexFeature.hasLyric) 1D else 0D)
    musicItem.setSongSearchPlayCount(statsFeature.songSearchPlayCount)
    musicItem.setSongArtistSearchPlayCount(statsFeature.songArtistSearchPlayCount)
    musicItem.setSongArtistSearchFinishRate(if (statsFeature.songArtistSearchPlayCount > 0)
        statsFeature.songArtistSearchFinishCount.toDouble / statsFeature.songArtistSearchPlayCount else 0D)
    musicItem.setSongArtistSearchCount(statsFeature.songArtistSearchCount)
    musicItem.setArtistSearchCount(artistFeature.artistSearchCount)
    musicItem.setArtistMusicCount(artistFeature.artistMusicCount)
    musicItem.setArtistOriginCount(artistFeature.artistOriginCount)
    if (artistFeature.artistVector != null) {
      musicItem.setArtistVector(artistFeature.artistVector.toList.map(_.asInstanceOf[java.lang.Double]).asJava)
    }
    if (coverArtistVector != null) {
      musicItem.setCoverArtistVector(coverArtistVector.toList.map(_.asInstanceOf[java.lang.Double]).asJava)
    }
    if (indexFeature.tags != null) {
      musicItem.setTags(indexFeature.tags.toList.asJava)
    }
    musicItem
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
