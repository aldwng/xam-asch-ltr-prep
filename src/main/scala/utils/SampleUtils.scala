package utils

import com.xiaomi.miui.ad.appstore.feature.Sample
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object SampleUtils {
  def generateRankSamples(sc: SparkContext, samples: RDD[Sample]): RDD[String] = {
    val data = samples
      .filter(x => x.isSetFeatures && x.isSetQid)
      .map { sample =>
        val features = sample.getFeatures.asScala
          .map(f => f.getId -> f.getValue)
          .filter(x => Math.abs(x._2) >= 0.0001)
        val label = sample.isSetLabel match {
          case true => sample.getLabel
          case false => 0
        }
        (label, sample.getQid, features, sample.getCommon)
      }
      .filter(_._3.size > 0)

    return data
      // .repartition(partition)
      .sortBy(_._2)
      .map {
        case (label, qid, features, common) =>
          val text = features
            .sortBy(_._1)
            .map {
              case (id, value) =>
                "%d:%.4f".format(id, value)
            }
            .mkString(" ")
          s"${label} qid:${qid} ${text} # ${common}"
      }
  }
}
