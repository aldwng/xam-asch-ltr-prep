package utils

import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.JavaConverters._


object TextUtils extends Serializable {

  val natures = Array("n","nr","nr1","nr2","nrj","nrf","ns","nsf","nt","nt","nl","ng","nw", "v","vd","vn","vf","vx","vi", "vg")

  val nonNumericRegex = "[^0-9]*".r

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

  def isUrl(word: Option[String]): Boolean = {
    word match {
      case Some(word) => {
        if (word.startsWith("http"))
          true
        else if (word.startsWith("www."))
          true
        else if (word.endsWith(".com"))
          true
        else if (word.endsWith(".cn"))
          true
        false
      }
      case None => false
    }
  }

  def isNumber(word: Option[String]): Boolean = {
    word match {
      case Some(word) => {
        word.trim.forall { x =>
          ('0' <= x && x <= '9') || x == '.'
        }
      }
      case None => false
    }
  }
}
