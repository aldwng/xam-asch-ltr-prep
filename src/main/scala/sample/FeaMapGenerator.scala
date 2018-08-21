package sample

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature.{FeaMap, Sample}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.PathUtils._

import scala.collection.JavaConverters._

object FeaMapGenerator {
  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val dev = args.getOrElse("dev", "false").toBoolean
    val day = semanticDate(args.getOrElse("day", "-1"))

    var inputPath = IntermediateDatePath(sample_path, day.toInt)
    var feaPath = IntermediateDatePath(fea_map_path, day.toInt)
    var feaTextPath = IntermediateDatePath(fea_text_path, day.toInt)

    var conf = new SparkConf()
      .setAppName("Fea Map Job")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec", "snappy")

    if (dev) {
      inputPath = sample_path_local
      feaPath = fea_map_path_local
      feaTextPath = fea_text_path_local
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val fs = FileSystem.get(new Configuration())
    val sc = spark.sparkContext
    val paths = Seq("/train", "/validate", "/test")
    val samples = paths
      .map(path => {
        sc.thriftParquetFile(inputPath + path, classOf[Sample])
      })
      .reduceLeft((x, y) => x ++ y)
      .filter(x => x.isSetFeatures)

    generate(sc, samples, feaPath, feaTextPath, fs)
    spark.stop()
    println("Job done!")
  }

  def generate(sc: SparkContext, samples: RDD[Sample], feaPath: String, feaTextPath: String, fs: FileSystem): Unit = {
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

    fs.delete(new Path(feaPath), true)
    feaMap.saveAsParquetFile(feaPath)

    fs.delete(new Path(feaTextPath), true)
    feaMap.saveAsTextFile(feaTextPath)
  }
}
