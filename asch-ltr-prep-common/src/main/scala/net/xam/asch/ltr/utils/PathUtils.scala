package net.xam.asch.ltr.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.nio.file.Paths

/**
  * @author aldwang
  */
object PathUtils {

  private val DATE_PATTERN = "yyyyMMdd"
  private val TIME_PATTERN = "HH:mm:ss"
  private val DATE_TIME_PATTERN = s"$DATE_PATTERN $TIME_PATTERN"

  def pathJoin(paths: String*): String = {
    java.nio.file.Paths.get(paths.head, paths.tail: _*).toString
  }

  def timestampToDateTime(ts: Long): String = {
    new DateTime(ts).toDateTime.toString(DATE_TIME_PATTERN)
  }

  def timestampToDate(ts: Long): String = {
    new DateTime(ts).toDateTime.toString(DATE_PATTERN)
  }

  def timestampToTime(ts: Long): String = {
    new DateTime(ts).toDateTime.toString(TIME_PATTERN)
  }

  private def parseDate(dateStr: String, pattern: String = "yyyyMMdd", offsetDays: Int = 0) = {
    val formatter = DateTimeFormat.forPattern(pattern)
    formatter.parseDateTime(dateStr).plusDays(offsetDays)
  }

  def parseDateTime(dateTimeStr: String, pattern: String = "yyyyMMdd HH:mm:ss") = {
    val formatter = DateTimeFormat.forPattern(pattern)
    formatter.parseDateTime(dateTimeStr)
  }

  private val DATE_RE = """(\d{8})""".r
  private val REL_DATE_RE = """-(\d+)""".r

  def semanticDate(str: String, pattern: String = "yyyyMMdd") = str match {
    case DATE_RE(s) => s
    case REL_DATE_RE(s) => new DateTime().minusDays(s.toInt).toString(pattern)
    case "today" => new DateTime().toString(pattern)
    case "yesterday" => new DateTime().minusDays(1).toString(pattern)
  }

  private def intervalDates(start: String, end: String, pattern: String = "yyyyMMdd") = {
    val startDate = parseDate(start, pattern)
    val endDate = parseDate(end, pattern)
    for {
      i <- 0 to Days.daysBetween(startDate, endDate).getDays
    } yield {
      val date = startDate.plusDays(i)
      ("%04d".format(date.getYear), "%02d".format(date.getMonthOfYear), "%02d".format(date.getDayOfMonth))
    }
  }

  private def rawDateIntervalPath(path: String, start: String, end: String): Seq[String] = {
    intervalDates(start, end, "yyyyMMdd").map(date => {
      java.nio.file.Paths.get(path, s"year=${date._1}", s"month=${date._2}", s"day=${date._3}").toString
    })
  }

  def rawDatePath(path: String, date: Int): String = {
    rawDateIntervalPath(path, date.toString, date.toString).head
  }

  def intermediateDateIntervalPath(path: String, start: String, end: String): Seq[String] = {
    intervalDates(start, end, "yyyyMMdd").map(date => {
      Paths.get(path, s"date=${date._1}${date._2}${date._3}").toString
    })
  }

  def intermediateDatePath(path: String, date: Int): String = {
    intermediateDateIntervalPath(path, date.toString, date.toString).head
  }
}
