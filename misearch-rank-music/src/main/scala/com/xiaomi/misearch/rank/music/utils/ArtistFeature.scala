package com.xiaomi.misearch.rank.music.utils

import com.xiaomi.data.spec.log.tv.MusicMetaMid
import org.apache.commons.collections.CollectionUtils

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

final case class ArtistFeature(artistId: Long,
                               artistName: String,
                               artistMusicCount: Long,
                               artistOriginCount: Long,
                               artistSearchCount: Long,
                               artistVector: Array[Double])

object ArtistFeature {

  def generateArtistFeatureFromMulti(artistFeatures: ListBuffer[ArtistFeature]): ArtistFeature = {
    if (artistFeatures.isEmpty) {
      return null
    }
    if (artistFeatures.length == 1) {
      return artistFeatures.head
    }
    val length = artistFeatures.length
    var artistId = 0L
    val artistNames = ListBuffer[String]()
    var artistMusicCount = 0L
    var artistOriginCount = 0L
    var artistSearchCount = 0L
    var artistVector = Array.fill(32)(0D)
    artistFeatures.foreach(af => {
      artistId += af.artistId
      artistNames.append(af.artistName)
      artistMusicCount += af.artistMusicCount
      artistOriginCount += af.artistOriginCount
      artistSearchCount += af.artistSearchCount
      if (af.artistVector != null) {
        artistVector = (artistVector, af.artistVector).zipped.map(_ + _)
      }
    })
    ArtistFeature(artistId / length, artistNames.mkString(";"), artistMusicCount / length, artistOriginCount / length,
      artistSearchCount / length, artistVector.map(_ / length))
  }

  def countArtistInfoFromIndex(metaMid: MusicMetaMid): ListBuffer[(Long, (Long, Long))] = {
    val items = ListBuffer[(Long, (Long, Long))]()
    if (CollectionUtils.isEmpty(metaMid.artistIds) || CollectionUtils.isEmpty(metaMid.resources)) return items
    val isOrigin = isOriginal(metaMid)
    metaMid.artistIds.asScala.foreach(id => {
      items.append(id.toLong -> (1L, if (isOrigin) 1L else 0L))
    })
    items
  }

  private def isOriginal(metaMid: MusicMetaMid): Boolean = {
    if (CollectionUtils.isEmpty(metaMid.resources)) return false
    metaMid.resources.asScala.foreach(res => {
      if (res.isOriginal== 1) {
        return true
      }
      if (CollectionUtils.isNotEmpty(res.tags)) {
        res.tags.asScala.foreach(ti => {
          if (ti.name == "原唱") {
            return true
          }
        })
      }
    })
    false
  }

}
