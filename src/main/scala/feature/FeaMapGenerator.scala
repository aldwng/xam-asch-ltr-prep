package feature

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature.{FeaMap, Sample}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.PathUtils._

import scala.collection.JavaConverters._

/**
  * Created by yun on 18-1-16.
  */
object FeaMapGenerator {
  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day  = semanticDate(args.getOrElse("day", "-1"))

    val rootPath = IntermediateDatePath(base_path, day.toInt)

    val inputPath   = rootPath + "/train/base/"
    val feaPath     = rootPath + "/feaMap"
    val feaTextPath = rootPath + "/feaText"

    val conf = new SparkConf()
      .setAppName("Fea Map Job")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec", "snappy")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc    = spark.sparkContext
    val paths = Seq("train", "validate", "test")
    val samples = paths
      .map { path =>
        sc.thriftParquetFile(inputPath + path, classOf[Sample])
      }
      .reduceLeft((x, y) => x ++ y)
      .filter(x => x.isSetFeatures)

    generate(sc, samples, feaPath, feaTextPath)
    spark.stop()
    println("Job done!")
  }

  def generate(sc: SparkContext, samples: RDD[Sample], feaPath: String, feaTextPath: String): Unit = {
    val feaMap = samples
      .flatMap(_.getFeatures.asScala)
      .map(_.getFea)
      .distinct()
      .zipWithIndex()
      .map { it =>
        val ans = new FeaMap()
        ans.setId(it._2 + 1)
        ans.setFea(it._1)
        ans
      }
      .repartition(1)
    feaMap.saveAsParquetFile(feaPath)
    feaMap.saveAsTextFile(feaTextPath)
  }

}
