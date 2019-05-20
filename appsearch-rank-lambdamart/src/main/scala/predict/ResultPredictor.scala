package predict

import com.twitter.scalding.Args
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature.QueryMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.PathUtils._
import utils.RankLibUtils._

object ResultPredictor {
  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val day = semanticDate(args.getOrElse("day", "-1"))
    val dev = args.getOrElse("dev", "false").toBoolean
    val norimalize = args.getOrElse("norimalize", "zscore")

    var modelPath = model_path
    var samplePath = IntermediateDatePath(predict_sample_path, day.toInt)
    var queryMapPath = IntermediateDatePath(predict_query_map_path, day.toInt)
    var outputPath = IntermediateDatePath(predict_rank_path, day.toInt)

    var conf = new SparkConf().setAppName("Predict Job")
    if (dev) {
      modelPath = model_path_local
      samplePath = predict_sample_path_local
      queryMapPath = predict_query_map_path_local
      outputPath = predict_rank_path_local
      conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    val modelText = sc.textFile(modelPath, 1).collect().mkString("\n")
    val bcText = sc.broadcast(modelText)

    val predictResult = sc
      .textFile(samplePath)
      .map(line => line.split(" ")(1) -> line)
      .groupByKey()
      .mapPartitions { it =>
        val model = loadModelFromString(bcText.value)
        it.flatMap {
          case (qid, line) =>
            predict(model, line.toSeq, true, true, norimalize)
        }
      }

    val queryMap = sc
      .thriftParquetFile(queryMapPath, classOf[QueryMap])
      .map(line => line.getId -> line.getQuery)
      .collectAsMap()

    val preResult = predictResult
      .map(line => line.split("\t"))
      .filter(line => line.length == 4)
      .map(line => line(0).toLong -> (line(2).toDouble, line(3).split(" ")(1).toLong))
      .groupByKey()
      .mapValues(v => v.toSeq.sortBy(-_._1))

    val rankData = preResult
      .map { x =>
        var result = s"${queryMap(x._1)},"
        for (i <- x._2.indices) {
          result += s"${x._2(i)._2}:${x._2(i)._1};"
        }
        result
      }

    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(outputPath), true)

    rankData.repartition(1)
      .saveAsTextFile(outputPath)
  }
}
