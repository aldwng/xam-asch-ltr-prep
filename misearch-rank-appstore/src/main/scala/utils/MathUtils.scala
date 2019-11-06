package utils

/**
  * @author Shenglan Wang
  */
object MathUtils {

  def computeStandardDeviation(list: Seq[Int], avg: Double, len: Int): Double = {
    val variance = list
      .map { x =>
        (x - avg) * (x - avg)
      }
      .reduceLeft(_ + _)
    Math.sqrt(variance / len)
  }
}
