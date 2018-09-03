package utils

import java.nio.file.Paths
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

object PathUtils {

  val base_path = "/user/h_misearch/appmarket/rank"
  val base_path_local = "/tmp/appsearch"

  val app_data_path = "/user/h_misearch/appmarket/pipeline_data/app_common/app_common.txt"
  val app_data_path_local = base_path_local + "/base/app_common.txt"

  val category_path = base_path + "/base/category.txt"
  val category_path_local = base_path_local + "/base/category.txt"

  val app_data_parquet_path = base_path + "/base/app"
  val app_data_parquet_path_local = base_path_local + "/base/app"

  val app_ext_parquet_path = base_path + "/base/app_ext"
  val app_ext_parquet_path_local = base_path_local + "/base/app_ext"

  val query_ext_path = base_path + "/base/query_ext"
  val query_ext_path_local = base_path_local + "/base/query_ext"

  val query_map_path = base_path + "/train/query_map"
  val query_map_path_local = base_path_local + "/train/query_map"

  val date_raw_path = base_path + "/train/data_raw"
  val data_raw_path_local = base_path_local + "/train/data_raw"

  val rank_instance_path = base_path + "/train/rank_instance"
  val rank_instance_path_local = base_path_local + "/train/rank_instance"

  val sample_path = base_path + "/train/sample"
  val sample_path_local = base_path_local + "/train/sample"

  val fea_map_path = base_path + "/train/fea_map"
  val fea_map_path_local = base_path_local + "/train/fea_map"

  val fea_text_path = base_path + "/train/fea_text"
  val fea_text_path_local = base_path_local + "/train/fea_text"

  val rank_sample_path = base_path + "/train/rank_sample"
  val rank_sample_path_local = base_path_local + "/train/rank_sample"

  val appstore_content_stats_path = "/user/h_data_platform/platform/appstore/appstore_content_statistics"
  val appstore_content_stats_path_local = base_path_local + "/stats/content"

  val download_history_path = "/user/h_misearch/appmarket/pipeline_data/app_ctr/download_history.txt"
  val download_history_path_local = base_path_local + "/download_history.txt"

  val model_path = base_path + "/model/model.txt"
  val model_path_local = base_path_local + "/model/model.txt"

  val natural_results_path = base_path + "/predict/natural_results.txt"
  val natural_results_path_local = base_path_local + "/predict/natural_results.txt"

  val predict_base_path = base_path + "/predict/base"
  val predict_base_path_local = base_path_local + "/predict/base"

  val predict_query_map_path = base_path + "/predict/query_map"
  val predict_query_map_path_local = base_path_local + "/predict/query_map"

  val predict_rank_path = base_path + "/predict/rank"
  val predict_rank_path_local = base_path_local + "/predict/rank"

  val predict_sample_path = base_path + "/predict/sample"
  val predict_sample_path_local = base_path_local + "/predict/sample"

  val predict_rank_unified_path = base_path + "/predict/rank_unified"
  val predict_rank_unified_path_local = base_path_local + "/predict/rank_unified"

  val predict_rank_merged_path = base_path + "/predict/rank_merged"
  val predict_rank_merged_path_local = base_path_local + "/predict/rank_merged"

  val DATE_PATTERN = "yyyyMMdd"
  val TIME_PATTERN = "HH:mm:ss"
  val DATE_TIME_PATTERN = s"$DATE_PATTERN $TIME_PATTERN"

  def parseQuery(word: String): String = {
    word.replace(" ", "").replaceAll("[\\pP‘’“”]", "").toLowerCase
  }

  def isNumber(word: String): Boolean = {
    word.trim.forall { x =>
      ('0' <= x && x <= '9') || x == '.'
    }
  }

  def nonBlank(s: String): Boolean = {
    s != null && s.trim.nonEmpty && s != "null" && s != "NULL"
  }

  def pathJoin(paths: String*): String = {
    Paths.get(paths.head, paths.tail: _*).toString
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

  def parseDate(dateStr: String, pattern: String = "yyyyMMdd", offsetDays: Int = 0) = {
    val formatter = DateTimeFormat.forPattern(pattern)
    formatter.parseDateTime(dateStr).plusDays(offsetDays)
  }

  def parseDateTime(dateTimeStr: String, pattern: String = "yyyyMMdd HH:mm:ss") = {
    val formatter = DateTimeFormat.forPattern(pattern)
    formatter.parseDateTime(dateTimeStr)
  }

  val DATE_RE = """(\d{8})""".r
  val REL_DATE_RE = """-(\d+)""".r

  def semanticDate(str: String, pattern: String = "yyyyMMdd") = str match {
    case DATE_RE(s) => s
    case REL_DATE_RE(s) => new DateTime().minusDays(s.toInt).toString(pattern)
    case "today" => new DateTime().toString(pattern)
    case "yesterday" => new DateTime().minusDays(1).toString(pattern)
  }

  def intervalDates(start: String, end: String, pattern: String = "yyyyMMdd") = {
    val startDate = parseDate(start, pattern)
    val endDate = parseDate(end, pattern)
    for {
      i <- 0 to Days.daysBetween(startDate, endDate).getDays
    } yield {
      val date = startDate.plusDays(i)
      ("%04d".format(date.getYear), "%02d".format(date.getMonthOfYear), "%02d".format(date.getDayOfMonth))
    }
  }

  def RawDateIntervalPath(path: String, start: String, end: String): Seq[String] = {
    intervalDates(start, end, "yyyyMMdd").map(date => {
      Paths.get(path, s"year=${date._1}", s"month=${date._2}", s"day=${date._3}").toString
    })
  }

  def RawDatePath(path: String, date: Int): String = {
    RawDateIntervalPath(path, date.toString, date.toString).head
  }

  def IntermediateDateIntervalPath(path: String, start: String, end: String): Seq[String] = {
    intervalDates(start, end, "yyyyMMdd").map(date => {
      Paths.get(path, s"date=${date._1}${date._2}${date._3}").toString
    })
  }

  def IntermediateDatePath(path: String, date: Int): String = {
    IntermediateDateIntervalPath(path, date.toString, date.toString).head
  }
}
