package net.xam.ltr.prep.run.utils

import scala.collection.mutable

object LogConstants {

  val LOG_DEVICE_SOUNDBOX = "device/soundbox"
  val LOG_DEVICE_TV = "device/tv"
  val LOG_DEVICE_VOICE = "app/assistant"
  val LOG_DOMAIN_MUSIC = "music"
  val LOG_DOMAIN_VOICE = "voice"
  val LOG_DOMAIN_JOKE = "joke"
  val LOG_SCREEN_SIZE_HEADLESS = "headless"

  val INTENTION_SLOT_SEP = "##"
  val INTENTION_SLOT_TYPE_SEP = "->"
  val INTENTION_SLOT_EMPTY = "null"

  val PROP_INTENTION_MULTI_INTENTION = "multiIntentions"
  val PROP_INTENTION_QUERY = "query"
  val PROP_INTENTION_SONG = "song"
  val PROP_INTENTION_ARTIST = "artist"
  val PROP_INTENTION_ALBUM = "album"
  val PROP_INTENTION_TAG = "tag"
  val PROP_INTENTION_KEYWORD = "keyword"
  val PROP_INTENTION_DISPLAY_SONG = "display_song"
  val PROP_INTENTION_DISPLAY_ARTIST = "display_artist"
  val PROP_INTENTION_DISPLAY_ALBUM = "display_album"
  val PROP_INTENTION_TYPE = "type"
  val PROP_INTENTION_OPT_INTENTION = "opt_intention"
  val PROP_INTENTION_FUNC = "func"

  val PROP_CONTENT_MUSICS = "musics"
  val PROP_CONTENT_DIRECTIVE = "directive"
  val PROP_CONTENT_IS_FOUND = "isFound"
  val PROP_CONTENT_MEMBER_LEVEL = "member_level"
  val PROP_CONTENT_SOURCE = "source"
  val PROP_CONTENT_VOICE_MUSICS = "voiceAssistantMusics"

  val PROP_MUSIC_MARK_INFO = "markInfo"
  val PROP_MUSIC_EID = "eid"
  val PROP_MUSIC_ID = "id"
  val PROP_MUSIC_SID = "sid"
  val PROP_MUSIC_GLOBAL_ID = "global_id"
  val PROP_MUSIC_SONG = "song"
  val PROP_MUSIC_ARTIST = "artist"
  val PROP_MUSIC_ALBUM_NAME = "album_name"
  val PROP_MUSIC_TAGS = "tags"
  val PROP_MUSIC_REFER = "refer"
  val PROP_MUSIC_ORIGIN = "origin"
  val PROP_MUSIC_CP = "cp"
  val PROP_MUSIC_SONG_ALIAS = "song_alias"
  val PROP_MUSIC_DURATION = "duration"
  val PROP_MUSIC_IS_VIP = "is_vip"

  val PROP_DIRECTIVE_ITEMS = "items"
  val PROP_DIRECTIVE_EXTEND = "extend"

  val PROP_ITEM_CP = "cp"
  val PROP_ITEM_TITLE = "title"
  val PROP_ITEM_ALBUM = "album"

  val MARK_BACKUP_INTENTION = "backup_intention"
  val MARK_LAMBDAMART = "lambdamart"
  val MARK_FINE_HISTORY = "fine_history"
  val MARK_SWITCH_HISTORY = "switch_history"
  val MARK_QQ_VIP = "qq_vip"

  val EID_BLACKBOX = "blackbox"
  val EID_RADICAL = "radical"
  val EID_GENERAL = "general"
  val EID_RADICAL_C4 = "radical_c4"
  val EID_RADICAL_CUBE_HISTORY = "radical_cube_history"
  val EID_RADICAL_TMP = "radical_tmp"
  val EID_RADICAL_REC = "radical_rec"
  val EID_SEARCH = ".*(blackbox|general|radical)+.*"
  val EID_S_A_BASE = "sABase"
  val EID_S_A_BASE_V9 = "sABaseV9"

  val CP_MIUI = "miui"
  val CP_XIAOWEI = "xiaowei"
  val CP_XIMALAYA = "ximalaya"
  val QQ = "qq"

  val PLAY_INFO_MANSWITCH = mutable.Set("manswitch", "medialistswitch")
  val PLAY_INFO_MAN_SWITCH = "manswitch"
  val PLAY_INFO_AUTOSWITCH = "autoswitch"
  val PLAY_INFO_PAUSE = "pause"
  val REFER_SEARCH = "search"
  val INTENTION_TYPE_MV = "mv"
  val MV_PREFIX = "mv"
  val MARK_INFO_SEP = "::"
  val SLOT_INFO_SEP = ";"
  val SLOT_SAME_PINYIN_SEP = "|"
  val SLOT_NEGATIVE_PREFIX = "!"
  val CP_SONG_ID_SEP = "_"
  val CP_SONG_ID_MIUI_PREFIX = "mi"
  val MEMBER_LEVEL_QQ_GREEN = "qq_green_vip"

  val CP_SOUNDBOX = mutable.Set("xiaowei", "xiaoai", "mi", "miui")

  //Here is not for ai_content_front_back_log
  val DATA_SEP = "->"
  val MUSIC_FIELD_SEP = "<+>"

  val DOMAIN = "domain"
  val TRACE_ID = "traceId"
  val SWITCH_TYPE = "switchType"
  val OFFSET = "offset"
  val START_TIME = "startTime"
  val END_TIME = "endTime"
}
