package com.xiaomi.misearch.rank.music.prepare.artist

import com.xiaomi.misearch.rank.music.common.model.artist.ArtistMusicItem
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object LabelGenerator {

  def generateLabel(data: RDD[(String, List[ArtistMusicItem])]): RDD[(String, ListBuffer[(ArtistMusicItem, Int)])] = {
    data
      .map {
        case (query, musicItems) =>
          query -> calcLabel(musicItems).sortWith(_._2 > _._2)
      }
  }

  private def calcLabel(musicItems: List[ArtistMusicItem]): ListBuffer[(ArtistMusicItem, Int)] = {

    val countBucketIndex = (musicItems.length * 0.3).toInt
    val rateBucketIndex = (musicItems.length * 0.4).toInt
    val sspcSplit = musicItems.sortWith(_.getSongSearchPlayCount > _.getSongSearchPlayCount)(countBucketIndex)
      .getSongSearchPlayCount
    val ssfrSplit = musicItems
      .sortWith(_.getSongSearchLabelFinishRate > _.getSongSearchLabelFinishRate)(rateBucketIndex)
      .getSongSearchLabelFinishRate
    val saspcSplit = musicItems.sortWith(_.getSongArtistSearchPlayCount > _.getSongArtistSearchPlayCount)(countBucketIndex)
      .getSongArtistSearchPlayCount
    val sasfrSplit = musicItems
      .sortWith(_.getSongArtistSearchLabelFinishRate > _.getSongArtistSearchLabelFinishRate)(rateBucketIndex)
      .getSongArtistSearchLabelFinishRate
    val asfrSplit = musicItems
      .sortWith(_.getArtistSearchLabelFinishRate > _.getArtistSearchLabelFinishRate)(rateBucketIndex)
      .getArtistSearchLabelFinishRate

    val musicItemList = ListBuffer[(ArtistMusicItem, Int)]()
    musicItems.foreach(item => {
      var label = 0
      if (item.getSongSearchPlayCount > sspcSplit) {
        label += 1
      }
      if (item.getSongSearchLabelFinishRate > ssfrSplit) {
        label += 1
      }
      if (item.getSongArtistSearchPlayCount > saspcSplit) {
        label += 1
      }
      if (item.getSongArtistSearchLabelFinishRate > sasfrSplit) {
        label += 1
      }
      if (item.getArtistSearchLabelFinishRate > asfrSplit) {
        label += 1
      }
      musicItemList.append((item, label))
    })
    musicItemList
  }
}
