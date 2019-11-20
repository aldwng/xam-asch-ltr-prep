package com.xiaomi.misearch.rank.music.utils

import com.xiaomi.data.spec.log.tv.MusicMetaMid
import com.xiaomi.misearch.rank.music.common.model.ArtistItem
import org.apache.commons.collections.CollectionUtils
import scala.collection.JavaConverters._

import scala.collection.mutable.ListBuffer

object ArtistItemUtils {

  def generateArtistFeatureFromMulti(artistFeatures: ListBuffer[ArtistItem]): ArtistItem = {
    if (artistFeatures.isEmpty) {
      return null
    }
    if (artistFeatures.length == 1) {
      return artistFeatures.head
    }
    val length = artistFeatures.length
    val artistNames = ListBuffer[String]()
    var artistMusicCount = 0L
    var artistOriginCount = 0L
    var artistSearchCount = 0L
    artistFeatures.foreach(af => {
      artistNames.append(af.getName)
      artistMusicCount += af.getMusicCount
      artistOriginCount += af.getOriginCount
      artistSearchCount += af.getSearchCount
    })
    new ArtistItem(artistNames.mkString(";"), artistSearchCount / length, artistMusicCount / length,
      artistOriginCount / length)
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
