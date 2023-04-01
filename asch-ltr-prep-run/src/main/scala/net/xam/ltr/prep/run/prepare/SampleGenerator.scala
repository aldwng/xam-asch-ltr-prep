package net.xam.ltr.prep.run.prepare

import com.twitter.scalding.Args
import net.xam.asch.ltr.common.model.{MusicItem, RankSample}
import net.xam.asch.ltr.common.utils.MusicFeatureUtils
import net.xam.asch.ltr.utils.PathUtils.{intermediateDatePath, semanticDate}
import net.xam.asch.ltr.utils.SerializationUtils
import net.xam.ltr.prep.run.utils.Paths
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/**
  * @author aldwang
  */
object SampleGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day = semanticDate(args.getOrElse("day", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var tagPath = Paths.SOUNDBOX_MUSIC_TAG_INDEX
    var musicFeaturePath = Paths.SOUNDBOX_MUSIC_FEATURE
    var labelPath = intermediateDatePath(Paths.LABEL_PATH, day.toInt)
    var zippedQueryOutputPath = intermediateDatePath(Paths.ZIPPED_QUERY_PATH, day.toInt)
    var sampleOutputPath = intermediateDatePath(Paths.SAMPLE_PATH, day.toInt)
    var conf = new SparkConf().setAppName(SampleGenerator.getClass.getName)

    if (dev) {
      tagPath = Paths.SOUNDBOX_MUSIC_TAG_INDEX_LOCAL
      musicFeaturePath = Paths.SOUNDBOX_MUSIC_FEATURE_LOCAL
      labelPath = Paths.LABEL_PATH_LOCAL
      zippedQueryOutputPath = Paths.ZIPPED_QUERY_PATH_LOCAL
      sampleOutputPath = Paths.SAMPLE_PATH_LOCAL
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    val tagMap = getTags(tagPath, sc).collectAsMap()
    val label = getLabels(labelPath, sc)
    val musicIdMap = sc.broadcast(label.map(_._2 -> 1).collectAsMap())
    val musicItemMap = sc.broadcast(getMusicItems(musicFeaturePath, musicIdMap, sc).collectAsMap())

    // Query
    val queries = label.map(_._1).distinct().zipWithIndex().map(x => x._1 -> (x._2 + 1).toInt)
    val queryMap = sc.broadcast(queries.collectAsMap())

    // Sample
    val samples = label.map {
      case (song, resourceId, label) =>
        (song, musicItemMap.value.get(resourceId), label, queryMap.value.get(song))
    }.filter(_._2.isDefined).filter(_._4.isDefined)
      .map {
        case (song, musicItemOpt, label, qidOpt) =>
          val musicItem = musicItemOpt.get
          val features = MusicFeatureUtils.extractFeatures(musicItem, tagMap.asJava)
          new RankSample(musicItem, song, qidOpt.get, label, features)
      }

    val fs = FileSystem.get(new Configuration())
    val queryDataSplits = queries.randomSplit(Array(0.9, 0.1))
    val train = sc.broadcast(queryDataSplits(0).collectAsMap())
    val test = sc.broadcast(queryDataSplits(1).collectAsMap())

    val trainOutputPath = sampleOutputPath + "/train"
    val testOutputPath = sampleOutputPath + "/test"
    fs.delete(new Path(trainOutputPath), true)
    fs.delete(new Path(testOutputPath), true)
    samples.filter(x => train.value.contains(x.getQuery)).sortBy(_.getQid, true, 1).map(MusicFeatureUtils.convertToText(_)).saveAsTextFile(trainOutputPath)
    samples.filter(x => test.value.contains(x.getQuery)).sortBy(_.getQid, true, 1).map(MusicFeatureUtils.convertToText(_)).saveAsTextFile(testOutputPath)

    fs.delete(new Path(zippedQueryOutputPath), true)
    sc.makeRDD(queries.sortBy(_._2).collect().map(_.productIterator.mkString("\t")), 1).repartition(1).saveAsTextFile(zippedQueryOutputPath)

    spark.stop()
    println("Job done!")
  }

  private def getLabels(labelPath: String, sc: SparkContext) = {
    sc.textFile(labelPath).map { x =>
      val splits = x.split("\t")
      val song = splits(0)
      val resourceId = splits(1)
      val label = splits(2).toInt
      (song, resourceId, label)
    }
  }

  private def getMusicItems(musicFeaturePath: String, musicIdMap: Broadcast[scala.collection.Map[String, Int]], sc: SparkContext) = {
    sc.textFile(musicFeaturePath).map { x =>
      SerializationUtils.fromJson(x, classOf[MusicItem])
    }.filter(x => musicIdMap.value.contains(x.getId)).map(x => x.getId -> x)
  }

  private def getTags(tagPath: String, sc: SparkContext) = {
    sc.textFile(tagPath).map { x =>
      val items = x.split("\t")
      if (items.length == 2) {
        (items(0), items(1))
      } else {
        ("", "")
      }
    }.filter(x => x._1.nonEmpty)
  }
}
