package utils

import java.nio.file.Paths
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

object PathUtils {

  val base_path = "/user/h_misearch/appmarket/rank"
  val base_path_local = "/tmp/appsearch"

  val app_data_parquet_path = base_path + "/base/app"
  val app_data_parquet_path_local = base_path_local + "/base/app"

  val app_data_path = "/user/h_misearch/appmarket/pipeline_data/app_common/app_common.txt"
  val app_data_path_local = base_path_local + "/base/app_common.txt"

  val download_history_path = "/user/h_misearch/appmarket/pipeline_data/app_ctr/download_history.txt"
  val appstore_content_stats_path        = "/user/h_data_platform/platform/appstore/appstore_content_statistics"
  val appstore_pv_path                   = "/user/h_data_platform/platform/appstore/appstore_pv_statistics"
  val app_active_log_path                = "/user/h_data_platform/platform/appstore/app_active_log_info"
  val app_tags_path                      = "matrix/relevance/app_tags"
  val exchange_bid_service_stat_log_path = "/user/h_scribe/miuiads/miuiads_exchange_bid_service_stat_log"
  val miui_ad_bid_billing_path           = "/user/h_data_platform/platform/miuiads/miui_ad_bid_billing"
  val misearch_global_news_hotquery_path = "/user/h_data_platform/platform/misearch/misearch_global_news_hotquery/data"

  val delivery_diagnosis_log_v2_path = "/user/h_scribe/miuiads/miuiads_delivery_diagnosis_log_v2"
  val ad_log_v2_path                 = "/user/h_scribe/miuiads/miuiads_ad_log_v2"
  val ad_event_path                  = "/user/h_data_platform/platform/miuiads/ad_event"

  val query_extender_path = "/user/h_data_platform/platform/miuiads/appstore_search/coclick_query_extender_v2"

  val ad_info_path = "/user/h_miui_ad/matrix/relevance/ad_info"

  val app_info_path         = "/user/h_data_platform/platform/appstore/appstore_appinfo/data/appinfo_to_hive_CN.txt"
  val app_info_parquet_path = "matrix/relevance/all_app"


  val filter_black_list = "matrix/relevance/filter/black_list"
  val filter_white_list ="matrix/relevance/filter/white_list"

  val app_expansion_path         = "/user/h_miui_ad/matrix/qu/app-expansion"
  val behavior_tags_keyword_path = "/user/h_miui_ad/matrix/warehouse/behavior_tags_keyword/std"

  val predict_query_path = "/user/h_miui_ad/develop/heqingquan/AppStore/AvaliableKeyword2SearchResultPage"

  val case_study_path              = base_path + "/case_study"
  val case_study_query_path        = case_study_path + "/query"
  val case_study_query_map_path    = case_study_path + "/query_map"
  val case_study_base_path         = case_study_path + "/base"
  val case_study_sample_path       = case_study_path + "/sample"
  val case_study_result_path       = case_study_path + "/result"
  val case_study_score_path        = case_study_path + "/score"
  val ruled_case_study_result_path = case_study_path + "/rule_result"

  val lookup_table_path           = base_path + "/lookup_table"
  val ruled_lookup_table_path     = base_path + "/ruled_lookup_table"
  val ruled_lookup_table_bak_path = base_path + "/ruled_lookup_table_bak"
  val boot_lookup_table_path      = base_path + "/boot_lookup_table"
  val gsearch_lookup_table_path     = base_path + "/gsearch_lookup_table"

  val natural_result_path           = base_path + "/natural_result"
  val natural_result_rawrank_path   = natural_result_path + "/rawrank"
  val natural_result_query_path     = natural_result_path + "/query"
  val natural_result_query_map_path = natural_result_path + "/query_map"
  val natural_result_base_path      = natural_result_path + "/base"
  val natural_result_sample_path    = natural_result_path + "/sample"
  val natural_result_rerank_path    = natural_result_path + "/rerank"

  val DATE_PATTERN      = "yyyyMMdd"
  val TIME_PATTERN      = "HH:mm:ss"
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

  val DATE_RE     = """(\d{8})""".r
  val REL_DATE_RE = """-(\d+)""".r

  def semanticDate(str: String, pattern: String = "yyyyMMdd") = str match {
    case DATE_RE(s)     => s
    case REL_DATE_RE(s) => new DateTime().minusDays(s.toInt).toString(pattern)
    case "today"        => new DateTime().toString(pattern)
    case "yesterday"    => new DateTime().minusDays(1).toString(pattern)
  }

  def intervalDates(start: String, end: String, pattern: String = "yyyyMMdd") = {
    val startDate = parseDate(start, pattern)
    val endDate   = parseDate(end, pattern)
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
