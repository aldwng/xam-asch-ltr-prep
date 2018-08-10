package sample

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature.{FeaMap, QueryMap, Sample}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.PathUtils._
import utils.SampleUtils

import scala.collection.JavaConverters._

/**
  * Created by yun on 18-1-16.
  */
object RankSampleGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args      = Args(mainArgs)
    val day       = semanticDate(args.getOrElse("day", "-1"))
    val partition = args.getOrElse("partition", "10").toInt
    val rootPath  = IntermediateDatePath(base_path, day.toInt)

    val inputPath  = rootPath + "/train/base/"
    val feaPath    = rootPath + "/feaMap"
    val queryPath  = rootPath + "/train/queryMap"
    val outputPath = rootPath + "/train/sample/"

    val conf = new SparkConf().setAppName("Rank Sample Job")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    val feaMap = sc
      .thriftParquetFile(feaPath, classOf[FeaMap])
      .map { it =>
        it.getFea -> it.getId
      }
      .collectAsMap()

    val queryMap = sc
      .thriftParquetFile(queryPath, classOf[QueryMap])
      .map { it =>
        it.getQuery -> it.getId
      }
      .collectAsMap()

    val fs    = FileSystem.get(new Configuration())
    val paths = Seq("train", "validate", "test")
    paths.foreach { path =>
      val samples = sc
        .thriftParquetFile(inputPath + path, classOf[Sample])
        .filter(x => x.isSetFeatures && x.isSetQuery && x.isSetLabel)
        .filter(x => queryMap.contains(x.getQuery))
        .map { sample =>
          val features = sample.getFeatures.asScala
            .filter(f => feaMap.contains(f.getFea) && f.getValue > 0)
            .map { f =>
              f.setId(feaMap(f.getFea))
            }
          sample.setFeatures(features.asJava)
          sample.setQid(queryMap(sample.getQuery))
          sample
        }
        .filter(x => x.getFeatures.size() > 0 && x.isSetQid)
        .repartition(partition)

      fs.delete(new Path(outputPath + path), true)
      SampleUtils.outputRankSamples(sc, samples, outputPath + path)
    }

    spark.stop()
    println("Job done!")
  }

}
