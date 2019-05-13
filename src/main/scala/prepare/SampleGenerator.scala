package prepare

import com.twitter.scalding.Args
import com.xiaomi.misearch.appsearch.rank.model.{App, RankSample}
import com.xiaomi.misearch.appsearch.rank.utils.{FeatureUtils, SampleUtils, SerializationUtils}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.PathUtils._

/**
  * @author Shenglan Wang
  */
object SampleGenerator {

  def main(mainArgs: Array[String]): Unit = {
    val args = Args(mainArgs)
    val dev = args.getOrElse("dev", "false").toBoolean
    val day = semanticDate(args.getOrElse("day", "-1"))

    var appDataPath = app_data_path
    var queryDataPath = query_data_path
    var labelPath = IntermediateDatePath(label_path, day.toInt)
    var sampleOutputPath = IntermediateDatePath(sample_path, day.toInt)
    var sampleTextOutputPath = IntermediateDatePath(sample_text_path, day.toInt)
    var conf = new SparkConf().setAppName(SampleGenerator.getClass.getName)

    if (dev) {
      appDataPath = app_data_path_local
      queryDataPath = query_data_path_local
      labelPath = label_path_local
      sampleOutputPath = sample_path_local
      sampleTextOutputPath = sample_text_path_local
      conf = conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    val appIdToAppMap = sc.broadcast(sc.textFile(appDataPath).map { line =>
      val app = SerializationUtils.fromJson(line, classOf[App])
      app.getRawId -> app
    }.collectAsMap())

    val queryToQidMap = sc.broadcast(sc.textFile(queryDataPath).map { line =>
      val splits = StringUtils.split(line, "\t")
      val query = splits(0)
      val qid = splits(1).toInt
      query -> qid
    }.collectAsMap())

    val fs = FileSystem.get(new Configuration())
    val paths = Seq("/train", "/test")
    paths.foreach {
      path =>
        val samples = generate(labelPath + path, appIdToAppMap, queryToQidMap, sc)

        val samplePath = sampleOutputPath + path
        fs.delete(new Path(samplePath), true)
        samples.map(s => SerializationUtils.toJson(s)).saveAsTextFile(samplePath)

        val sampleTextPath = sampleTextOutputPath + path
        fs.delete(new Path(sampleTextPath), true)
        samples.map(s => SampleUtils.convertToText(s)).saveAsTextFile(sampleTextPath)
    }
    spark.stop()
    println("Job done!")
  }

  def generate(samplePath: String, appIdToAppMap: Broadcast[scala.collection.Map[String, App]], queryToQidMap: Broadcast[scala.collection.Map[String, Int]], sc: SparkContext): RDD[RankSample] = {
    sc.textFile(samplePath).map { line =>
      val splits = StringUtils.split(line, "\t")
      val query = splits(0)
      val appId = splits(1)
      val label = splits(2).toInt

      val appOpt = appIdToAppMap.value.get(appId)
      val qidOpt = queryToQidMap.value.get(query)

      (query, appOpt, qidOpt, label)
    }.filter(x => x._2.isDefined && x._3.isDefined)
      .map(x => (x._1, x._2.get, x._3.get, x._4))
      .repartition(1)
      .sortBy(_._3)
      .map {
        case (query, app, qid, label) =>
          val features = FeatureUtils.extractFeatures(app, query)
          new RankSample(app, query, qid, label, features)
      }
  }
}
