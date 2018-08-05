package utils

/**
  * Created by wangjie5 on 2017/12/13.
  */
object FilterWords extends Serializable {

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
