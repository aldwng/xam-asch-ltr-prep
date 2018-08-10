package process

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature.DataRaw
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.PathUtils
import utils.PathUtils._
import utils.ProcessUtils._

import scala.collection.JavaConverters._


object DataRawGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val yesterday = semanticDate(args.getOrElse("end", "-1"))
    val rootPath = IntermediateDatePath(base_path, yesterday.toInt)
    val queryMapPath = rootPath + "/train/queryMap"
    val outputPath = rootPath + "/train/dataRaw"

    val conf = new SparkConf()
      .setAppName("Data Base Job")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec", "snappy")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    val downloadHistory = sc.textFile(PathUtils.download_history_path).flatMap(line => {
      val items = line.split("\t")
      val query = items(0)
      val downloadHistory = items(3)

      val result = downloadHistory.split(",").map(x => {
        val items = x.split(":")
        val appId = items(0)
        val downloadWeight = items(1).toLong
        val downloadCount = items(2).toLong
        (query, (appId, downloadWeight, downloadCount))
      })
      result
    })

    val dataRaw = generate(spark, downloadHistory)
    val queries = dataRaw.map(x => x.getQuery).distinct()
    saveAsQueryMap(queries, queryMapPath)

    val result = queries.map(_ -> 1).randomSplit(Array(0.8, 0.1, 0.1))
    val train = result(0).collectAsMap()
    val validate = result(1).collectAsMap()
    val test = result(2).collectAsMap()
    dataRaw.filter(x => train.contains(x.getQuery)).saveAsParquetFile(outputPath + "/train")
    dataRaw.filter(x => validate.contains(x.getQuery)).saveAsParquetFile(outputPath + "/validate")
    dataRaw.filter(x => test.contains(x.getQuery)).saveAsParquetFile(outputPath + "/test")

    spark.stop()
    println("Job done!")
  }

  def generate(spark: SparkSession, downloadHistory: RDD[(String, (String, Long, Long))]): RDD[DataRaw] = {
    val input = downloadHistory
      .map(x => {
        val query = parseQuery(x._1)
        val appId = x._2._1
        val downloadWeight = x._2._2
        val downloadCount = x._2._3
        (query, appId) -> (downloadWeight, downloadCount)
      })
      .filter(x => nonBlank(x._1._1))
      .reduceByKey((l, r) => (l._1 + r._1, l._2 + r._2))

    val queryStatMap = input
      .map {
        case ((query, _), (downloadWeight, _)) =>
          query -> downloadWeight
      }
      .groupByKey()
      .mapValues { values =>
        val len = values.size
        val avg = values.sum.toDouble / len
        val gap = values.map(x => (x - avg) * (x - avg)).sum
        val std = Math.sqrt(gap / len)
        (avg, std, values.max)
      }
      .collectAsMap()

    val labeledData = input.map {
      case ((query, appId), (downloadWeight, downloadCount)) =>
        val (avg, std, max) = queryStatMap(query)
        var label = 0
        if (downloadWeight == max) {
          label = 4
        } else if (downloadWeight > avg + std) {
          label = 3
        } else if (downloadWeight > avg) {
          label = 2
        } else if (downloadCount > 50) {
          label = 1
        } else {
          label = 0
        }
        (query, label, appId, downloadWeight, downloadCount)
    }

    // each query contains at least two label
    val queries = labeledData
      .map(x => x._1 -> x._2)
      .groupByKey()
      .filter(x => x._2.toSeq.distinct.size > 1)
      .map(x => x._1 -> 1)
      .collectAsMap()

    val result = labeledData.filter(x => queries.contains(x._1)).map {
      case (query, label, appId, downloadWeight, downloadCount) =>
        val dataRaw = new DataRaw()
        val seg = wordSegment(query)
        if (seg.size > 0) {
          dataRaw.setQuerySeg(seg.asJava)
        }
        dataRaw.setQuery(query)
        dataRaw.setAppId(appId.toLong)
        //        dataRaw.setExposure(exposure)
        dataRaw.setDownload(downloadCount.toInt)
        dataRaw.setDtr(downloadWeight)
        dataRaw.setLabel(label)
    }
    result
  }
}
