package sample

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature.{RankInstance, Sample}
import features.ExtractorBase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.PathUtils._

import scala.collection.JavaConverters._

object SampleGenerator {
  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day  = semanticDate(args.getOrElse("day", "-1"))

    val rootPath   = IntermediateDatePath(base_path, day.toInt)
    val inputPath  = rootPath + "/train/instance/"
    val outputPath = rootPath + "/train/base/"

    val conf = new SparkConf().setAppName("Train Samples Job")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc      = spark.sparkContext
    val fs = FileSystem.get(new Configuration())
    val paths = Seq("train", "validate", "test")
    paths.foreach{
      path =>
        val data    = sc.thriftParquetFile(inputPath + path, classOf[RankInstance])
        val samples = generate(data)
        fs.delete(new Path(outputPath + path), true)
        samples.saveAsParquetFile(outputPath + path)
    }
    spark.stop()
    println("Job done!")
  }

  def generate(data: RDD[RankInstance]): RDD[Sample] = {
    data.filter(it => it.isSetData && it.isSetApp && it.getData.isSetQuery).map { it =>
      val features = ExtractorBase.extractFeatures(it)
      val label = it.getData.isSetLabel match {
        case true  => it.getData.getLabel
        case false => 0
      }
      val ans = new Sample()
      ans.setQuery(it.getData.getQuery)
      ans.setLabel(label)
      ans.setFeatures(features.asJava)
      ans.setCommon(it.getApp.getAppId.toString)
    }
  }
}
