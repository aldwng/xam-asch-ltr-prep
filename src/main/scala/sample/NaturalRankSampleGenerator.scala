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

object NaturalRankSampleGenerator {
  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day = semanticDate(args.getOrElse("day", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean

    var feaPath = IntermediateDatePath(fea_map_path, day.toInt)
    var inputPath = IntermediateDatePath(predict_base_path, day.toInt)
    var queryPath = IntermediateDatePath(predict_query_map_path, day.toInt)
    var outputPath = IntermediateDatePath(predict_sample_path, day.toInt)

    val conf = new SparkConf().setAppName("Natural Result Rank Sample Job")

    if (dev) {
      feaPath = fea_map_path_local
      inputPath = predict_base_path_local
      queryPath = predict_query_map_path_local
      outputPath = predict_sample_path_local
      conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(outputPath), true)

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

    val samples = sc
      .thriftParquetFile(inputPath, classOf[Sample])
      .filter(x => x.isSetFeatures && x.isSetQuery)
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

    val result = SampleUtils.generateRankSamples(sc, samples)
    result.saveAsTextFile(outputPath)
    spark.stop()
    println("Job done!")
  }
}
