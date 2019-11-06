package utils

import ciir.umass.edu.features._
import ciir.umass.edu.learning.{RankList, Ranker, RankerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object RankLibUtils extends Serializable {

  def loadModelFromFile(modelFile: String): Ranker = {
    val rankerFactory = new RankerFactory
    rankerFactory.loadRankerFromFile(modelFile)
  }

  def loadModelFromString(modelText: String): Ranker = {
    val rankerFactory = new RankerFactory
    rankerFactory.loadRankerFromString(modelText)
  }

  // normType must in [sum, zscore, linear, log]
  def predict(model: Ranker, samples: Seq[String], isSparse: Boolean = true, isNorm: Boolean = true, normType: String = "sum"): Seq[String] = {
    val input = FeatureManager.readInput(samples.asJava, false, isSparse).asScala

    if (isNorm)
      normalize(input, normType)

    val results = new ListBuffer[String]
    input.foreach(x => {
      for (i <- 0 until x.size) {
        results.append(s"${x.getID}\t$i\t${model.eval(x.get(i))}\t${x.get(i).getDescription}")
      }
    })

    results.toList
  }

  private def normalize(input: Seq[RankList], normType: String): Unit = {
    var normalizer: Normalizer = new SumNormalizor
    if (normType.toLowerCase == "zscore")
      normalizer = new ZScoreNormalizor
    if (normType.toLowerCase == "linear")
      normalizer = new LinearNormalizer
    if (normType.toLowerCase == "log")
      normalizer = new LogNormalizor
    input.foreach(x => normalizer.normalize(x))
  }
}
