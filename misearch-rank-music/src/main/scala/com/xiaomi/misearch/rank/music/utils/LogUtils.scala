package com.xiaomi.misearch.rank.music.utils

object LogUtils {

  private def isMatchField(queries: Array[String], fields: Array[String]): Boolean = {
    queries.foreach(q => {
      fields.foreach(f => {
        if (q.equalsIgnoreCase(f)) {
          return true
        }
      })
    })
    false
  }

  def isMatchField(query: String, field: String): Boolean = {
    if (query == null) {
      return true
    }
    if (field == null) {
      return false
    }
    isMatchField(query.split(";"), field.split(";"))
  }

  def getIdFromMarkInfo(markInfo: String): String = {
    if (markInfo == null || !markInfo.contains("::")) {
      return null
    }
    val infos = markInfo.split("::")
    if (infos.nonEmpty) return infos(0)
    null
  }

  def combineIdAndCp(id: String, cp: String): String = {
    val availableCps = ".*(xiaowei|miui|xiaoai|beiwa)+.*"
    if (cp == null || !cp.matches(availableCps)) {
      return null
    }
    (if (cp == "miui") "mi" else cp) + "_" + id
  }
}
