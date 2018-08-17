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
    val dev = args.getOrElse("dev", "false").toBoolean
    val day       = semanticDate(args.getOrElse("day", "-1"))
    val partition = args.getOrElse("partition", "10").toInt

    var feaPath = IntermediateDatePath(fea_map_path, day.toInt)
    var queryPath = IntermediateDatePath(query_map_path, day.toInt)
    var inputPath = IntermediateDatePath(sample_path, day.toInt)
    var outputPath = IntermediateDatePath(rank_sample_path, day.toInt)

    var conf = new SparkConf().setAppName("Rank Sample Job")
    if(dev) {
      feaPath = fea_map_path_local
      queryPath =  query_map_path_local
      inputPath = sample_path_local
      outputPath = rank_sample_path_local
      conf.setMaster("local[*]")
    }

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
    val paths = Seq("/train", "/validate", "/test")
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
      val result = SampleUtils.generateRankSamples(sc, samples)
      result.saveAsTextFile(outputPath + path)
    }

    spark.stop()
    println("Job done!")
  }

}
