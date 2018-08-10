package utils

import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.miui.ad.appstore.feature._
import features.ExtractorBase
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object ProcessUtils {
  val natures = Array("n","nr","nr1","nr2","nrj","nrf","ns","nsf","nt","nt","nl","ng","nw", "v","vd","vn","vf","vx","vi", "vg")


  def saveAsQueryMap(query: RDD[String], outputPath: String): Unit = {
    val output = query.zipWithIndex().map {
      case (qy, id) =>
        val ans = new QueryMap()
        ans.setQuery(qy)
        ans.setId(id)
        ans
    }
    output.saveAsParquetFile(outputPath)
  }

  def computeStandardDeviation(list: Seq[Int], avg: Double, len: Int): Double = {
    val variance = list
      .map { x =>
        (x - avg) * (x - avg)
      }
      .reduceLeft(_ + _)
    Math.sqrt(variance / len)
  }

  val regexNum = "[^0-9]*".r

  def tokenize(content: Seq[String]): Seq[String] = {
    content
      .map(_.toLowerCase.trim)
      //.filter(regexNum.pattern.matcher(_).matches) //filter the word included number
      .filter(_.length > 1) //filter the word that it's length less than 2
  }

  def wordSegment(text: String): Seq[String] = {
    ToAnalysis
      .parse(text)
      .getTerms
      .asScala
      .map(_.getName)
      .filter(w => w.length > 1)
  }

  def textSegment(text: String): Seq[String] = {
    ToAnalysis
      .parse(text)
      .getTerms
      .asScala
      .filter(t=>natures.contains(t.getNatureStr))
      .map(_.getName)
      .filter(w => w.length > 1)
  }

}
