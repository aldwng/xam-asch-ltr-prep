package com.xiaomi.misearch.rank.music.utils

import com.google.gson.JsonObject
import com.xiaomi.data.spec.platform.misearch.{PlayInfoLog, SoundboxMusicSearchLog}
import com.xiaomi.misearch.rank.music.utils.AiContentLogUtils.getMusicProp

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

  private def getIdFromMarkInfo(markInfo: String): String = {
    if (markInfo == null || !markInfo.contains("::")) {
      return null
    }
    val infos = markInfo.split("::")
    if (infos.nonEmpty) infos(0) else null
  }

  private def combineIdAndCp(id: String, cp: String): String = {
    val availableCps = ".*(xiaowei|miui|xiaoai|beiwa)+.*"
    if (cp == null || !cp.matches(availableCps)) {
      return null
    }
    (if (cp == "miui") "mi" else cp) + "_" + id
  }

  def extractMusicId(x: SoundboxMusicSearchLog) = {
    val musicId = x.musicid
    val cp = x.cp

    val idFromLog = combineIdAndCp(musicId, cp)
    val idFromMark = getIdFromMarkInfo(x.markinfo)
    if (idFromMark == null && idFromLog == null) {
      ""
    } else {
      if (idFromMark != null) idFromMark else idFromLog
    }
  }

  def extractMusicId(piLog: PlayInfoLog, mObj: JsonObject): String = {
    val musicId = piLog.getResid
    val cp = piLog.getCp

    val idFromLog = combineIdAndCp(musicId, cp)
    val idFromMark = getIdFromMarkInfo(getMusicProp(mObj, "markInfo"))
    if (idFromMark == null && idFromLog == null) {
      ""
    } else {
      if (idFromMark != null) idFromMark else idFromLog
    }
  }

  def isFinish30s(log: SoundboxMusicSearchLog): Boolean = {
    val time = log.endtime - log.starttime
    if (time > 30 && time < 600) {
      return true
    }
    false
  }

  def isFinish30s(piLog: PlayInfoLog): Boolean = {
    val time = piLog.getEndtime - piLog.getStartime
    if (time > 30 && time < 600) {
      return true
    }
    false
  }
}
