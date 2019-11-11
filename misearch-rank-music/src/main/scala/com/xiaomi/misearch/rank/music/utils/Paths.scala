package com.xiaomi.misearch.rank.music.utils

/**
  * @author Shenglan Wang
  */
object Paths {

  val BASE_DIR = "/user/h_misearch/ai/music/lambdamart"
  val BASE_DIR_LOCAL = "/tmp/music"

  val MUSIC_SEARCH_LOG = "/user/h_data_platform/platform/misearch/soundbox_music_search_log"
  val MUSIC_SEARCH_LOG_LOCAL = BASE_DIR_LOCAL + "/search_log"

  val RAW_MUSIC_DATA_PATH = BASE_DIR + "/raw_music_data"
  val RAW_MUSIC_DATA_PATH_LOCAL = BASE_DIR_LOCAL + "/raw_music_data"

  val MUSIC_DATA_PATH = BASE_DIR + "/music_data"
  val MUSIC_DATA_PATH_LOCAL = BASE_DIR_LOCAL + "/music_data"

  val QUERY_PATH = BASE_DIR + "/query"
  val QUERY_PATH_LOCAL = BASE_DIR_LOCAL + "/query"

  val QQ_RANK_PATH = BASE_DIR + "/qq_rank.txt"
  val QQ_RANK_PATH_LOCAL = BASE_DIR_LOCAL + "/qq_rank.txt"

  val LABEL_PATH = BASE_DIR + "/label"
  val LABEL_PATH_LOCAL = BASE_DIR_LOCAL + "/label"

  val ZIPPED_QUERY_PATH = BASE_DIR + "/zip_query"
  val ZIPPED_QUERY_PATH_LOCAL = BASE_DIR_LOCAL + "/zip_query"
}
