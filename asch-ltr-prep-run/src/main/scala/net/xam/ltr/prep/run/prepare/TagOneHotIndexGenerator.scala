package net.xam.ltr.prep.run.prepare

import xmd.spec.log.tv.{MaterialMusicMid, MusicTagType, TableName}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import net.xam.ltr.prep.run.utils.Paths._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object TagOneHotIndexGenerator {
  def main(args: Array[String]) = {
    val dateTime = new DateTime()
    val idxPath = MATERIAL_MUSIC_INDEX_DATA + "/date=" + dateTime.minusDays(2).toString("yyyyMMdd")

    val sparkConf = new SparkConf().setAppName("TagOneHotIndexGenerator")
    val sparkContext = new SparkContext(sparkConf)

    val tags = sparkContext.thriftParquetFile(idxPath, classOf[MaterialMusicMid])
      .filter(_.tableName == TableName.MUSIC)
      .map(_.musicMetaMid)
      .filter(_.resources != null)
      .filter(_.resources.size() > 0)
      .flatMap {
        idx => {
          val lb = ListBuffer[(String, Long)]()
          idx.resources.asScala.foreach(r => {
            if (r.tags != null && r.tags.size() > 0) {
              r.tags.asScala.foreach(t => {
                if (t.`type` == MusicTagType.STYLE && !t.name.contains("best")) {
                  lb.append(t.name -> 1L)
                }
              })
            }
          })
          lb
        }
      }
      .filter(_ != null)
      .reduceByKey(_ + _)
      .filter(_._2 > 5000)

    val tagIndex = sparkContext.parallelize(
      tags.collect().map(_._1).zipWithIndex.toSeq
    )
      .sortBy(_._2, ascending = true, 1)
      .map {
        case (tag, index) =>
          tag + "\t" + index
      }


    val hadoopConf = new Configuration(sparkContext.hadoopConfiguration)
    val fs: FileSystem = FileSystem.get(hadoopConf)

    fs.delete(new Path(SOUNDBOX_MUSIC_TAG_INDEX), true)
    tagIndex.repartition(1).saveAsTextFile(SOUNDBOX_MUSIC_TAG_INDEX)

    sparkContext.stop()
  }
}