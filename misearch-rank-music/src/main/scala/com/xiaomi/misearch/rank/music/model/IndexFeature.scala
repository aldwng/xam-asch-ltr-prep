package com.xiaomi.misearch.rank.music.model

import com.xiaomi.data.spec.log.tv.{MusicMetaMid, MusicResourceMid}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConverters._

final case class IndexFeature (metaId: Long,
                               song: String,
                               artists: Array[String],
                               artistIds: Array[Long],
                               tags: Array[String],
                               metaRank: Double,
                               resRank: Double,
                               quality: Double,
                               qqSongRawRank: Int,
                               qqMultiArtistRawRank: String,
                               qqRank: Double,
                               vipType: Int,
                               hasLyric: Boolean,
                               coverArtist: String)

object IndexFeature {
  def getFeatureFromIndexTable(metaMid: MusicMetaMid, resMid: MusicResourceMid): IndexFeature = {
    var artists: Array[String] = null
    var artistIds: Array[Long] = null
    var tags: Array[String] = null
    if (CollectionUtils.isNotEmpty(metaMid.getArtistName)) {
      artists = metaMid.getArtistName.asScala.toArray
    }
    if (CollectionUtils.isNotEmpty(metaMid.getArtistIds)) {
      artistIds = metaMid.getArtistIds.asScala.toArray.map(_.toLong)
    }
    if (CollectionUtils.isNotEmpty(resMid.getTags)) {
      tags = resMid.getTags.asScala.toArray.filter(!_.name.contains("台湾民歌运动")).map(_.name)
    }
    IndexFeature(metaMid.getMusicId, metaMid.getName, artists, artistIds, tags,
      metaMid.getRank, resMid.getRank, resMid.getQuality, resMid.getQqSongRawRank, resMid.getQqMultiArtistRawRank,
      resMid.getQqRank, resMid.vipType, StringUtils.isNotBlank(metaMid.getLyricTxt), resMid.getFollow)
  }
}
