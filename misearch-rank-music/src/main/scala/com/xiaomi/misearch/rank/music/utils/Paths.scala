package com.xiaomi.misearch.rank.music.utils

/**
  * @author Shenglan Wang
  */
object Paths {

  val BASE_DIR = "/user/h_misearch/ai/music/lambdamart"
  val BASE_DIR_LOCAL = "/tmp/music"

  val SOUNDBOX_MUSIC_SEARCH_LOG = "/user/h_data_platform/platform/misearch/soundbox_music_search_log"
  val SOUNDBOX_MUSIC_SEARCH_LOG_LOCAL = BASE_DIR_LOCAL + "/search_log"

  val MUSIC_STAT_PATH = BASE_DIR + "/music_stat"
  val MUSIC_STAT_PATH_LOCAL = BASE_DIR_LOCAL + "/music_stat"

  val QUERY_PATH = BASE_DIR + "/query"
  val QUERY_PATH_LOCAL = BASE_DIR_LOCAL + "/query"

  val QQ_RANK_PATH = BASE_DIR + "/qq_rank.txt"
  val QQ_RANK_PATH_LOCAL = BASE_DIR_LOCAL + "/qq_rank.txt"

  val LABEL_PATH = BASE_DIR + "/label"
  val LABEL_PATH_LOCAL = BASE_DIR_LOCAL + "/label"

  val ZIPPED_QUERY_PATH = BASE_DIR + "/zip_query"
  val ZIPPED_QUERY_PATH_LOCAL = BASE_DIR_LOCAL + "/zip_query"

  val SAMPLE_PATH = BASE_DIR + "/sample"
  val SAMPLE_PATH_LOCAL = BASE_DIR_LOCAL + "/sample"

  val MATERIAL_MUSIC_INDEX_DATA = "/user/h_data_platform/platform/aiservice/ai_service_music_idx"
  val MATERIAL_MUSIC_INDEX_DATA_LOCAL = BASE_DIR_LOCAL + "/music_idx"

  val SOUNDBOX_SONG_ARTIST_PAIR = "/user/h_misearch/ai/music/table/soundbox_search/feedback_song_artist_pair"

  val ARTIST_EMBEDDING_VECTOR = "/user/h_sns/soundbox_recommend/mid_data/scenes_similar/user_artist_v1.vec"

  val SOUNDBOX_MUSIC_FEATURE = BASE_DIR + "/music_feature"
  val SOUNDBOX_MUSIC_FEATURE_LOCAL = BASE_DIR_LOCAL + "/music_feature"

  val SOUNDBOX_MUSIC_STORED_FEATURE = BASE_DIR + "/music_stored_feature"

  val SOUNDBOX_ARTIST_STORED_FEATURE = BASE_DIR + "/artist_stored_feature"

  val SOUNDBOX_MUSIC_TAG_INDEX = BASE_DIR + "/music_tag_index"
  val SOUNDBOX_MUSIC_TAG_INDEX_LOCAL = BASE_DIR_LOCAL + "/music_tag_index"

  val ARTIST_BASE_DIR = "/user/h_misearch/ai/music/lambdamart_artist"

  val ARTIST_SAMPLE_PATH = ARTIST_BASE_DIR + "/sample"

  val STORED_QUERY_STATS = ARTIST_BASE_DIR + "/stored_feature/query"

  val STORED_STATS_ITEMS = ARTIST_BASE_DIR + "/stored_feature/stats"
}
