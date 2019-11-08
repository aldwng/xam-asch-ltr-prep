package utils

object Paths {

  val base_path = "/user/h_misearch/appmarket/lambdarank"
  val base_path_local = "/tmp/appsearch"

  val app_common_path = "/user/h_misearch/appmarket/pipeline_data/app_common/app_common.txt"
  val app_common_path_local = base_path_local + "/app_common.txt"

  val category_path = base_path + "/category.txt"
  val category_path_local = base_path_local + "/category.txt"

  val app_data_path = base_path + "/app"
  val app_data_path_local = base_path_local + "/app"

  val label_path = base_path + "/label"
  val label_path_local = base_path_local + "/label"

  val query_data_path = base_path + "/query"
  val query_data_path_local = base_path_local + "/query"

  val sample_path = base_path + "/sample/sample"
  val sample_path_local = base_path_local + "/sample/sample"

  val sample_text_path = base_path + "/sample/text"
  val sample_text_path_local = base_path_local + "/sample/text"

  val appstore_content_stats_path = "/user/h_data_platform/platform/appstore/appstore_content_statistics"
  val appstore_content_stats_path_local = base_path_local + "/stats/content"

  val market_search_result_path = "/user/h_misearch/appmarket/crawler"
  val market_search_result_path_local = base_path_local

  val download_history_path = "/user/h_misearch/appmarket/pipeline_data/app_ctr/download_history.txt"
  val download_history_path_local = base_path_local + "/download_history.txt"

  val model_path = base_path + "/model/model.txt"
  val model_path_local = base_path_local + "/model/model.txt"

  var rank_diff_path = base_path + "/diff"
  var rank_diff_path_local = base_path_local + "/diff"

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
}
