package com.xiaomi.misearch.rank.music.utils

import com.google.gson.{JsonArray, JsonObject}
import com.xiaomi.data.spec.platform.misearch.{AiContentFrontBackLog, PlayInfoLog}
import com.xiaomi.misearch.rank.music.utils.LogConstants._
import com.xiaomi.misearch.rank.utils.GsonUtils._
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * @author wanglingda@xiaomi.com
 */

object AiContentLogUtils {

  val NON_NORMAL_SLOT_MARK = ".*(;|!|\\|)+.*"
  val NEG_MARK = "!"
  val HOMOPHONY_MARK_4_MATCH = "|"
  val HOMOPHONY_MARK_4_SPLIT = "\\|"
  val MULTI_MARK = ";"
  val SEARCH_REFERS = ".*(search|similarSearch|blackSearch)+.*"

  def isEffectiveMusicLog(log: AiContentFrontBackLog): Boolean = {
    log.getDeviceType == LOG_DEVICE_SOUNDBOX && log.getDomain == LOG_DOMAIN_MUSIC && log.getPlayInfoLog != null &&
      log.getIntention != null && log.getContent != null
  }

  def isHeadless(log: AiContentFrontBackLog): Boolean = {
    StringUtils.containsIgnoreCase(log.getScreenSize, LOG_SCREEN_SIZE_HEADLESS)
  }

  def isVoiceMusicLog(log: AiContentFrontBackLog): Boolean = {
    log.getDeviceType == LOG_DEVICE_VOICE && log.getDomain == LOG_DOMAIN_MUSIC && log.getIntention != null &&
      log.getContent != null
  }

  def isTvMusicLog(log: AiContentFrontBackLog): Boolean = {
    log.getDeviceType == LOG_DEVICE_TV && log.getDomain == LOG_DOMAIN_MUSIC && log.getIntention != null &&
      log.getContent != null
  }

  def isSongOrArtistSearch(logIntention: String): Boolean = {
    isSongOrArtistSearch(getJsonObject(logIntention))
  }

  def isArtistSearch(intention: String): Boolean = {
    isArtistSearch(getJsonObject(intention))
  }

  def isSongSearch(intention: String): Boolean = {
    isSongSearch(getJsonObject(intention))
  }

  def isPrecise(intention: String): Boolean = {
    isPrecise(getJsonObject(intention))
  }

  def isPrecise(iObj: JsonObject): Boolean = {
    if (iObj == null) {
      return false
    }
    val song = getStrProp(iObj, PROP_INTENTION_SONG)
    isNormalSlot(song)
  }

  def hasIntention(intention: String, slot: String): Boolean = {
    hasIntention(getJsonObject(intention), slot)
  }

  def hasIntention(intentionObj: JsonObject, slot: String): Boolean = {
    if (intentionObj == null || StringUtils.isBlank(slot)) {
      return false
    }
    val intentionSlot = getStrProp(intentionObj, slot)
    isNormalSlot(intentionSlot)
  }

  def isEmptySlot(intentionObj: JsonObject): Boolean = {
    if (intentionObj == null) {
      return true
    }
    isBlankStrProp(intentionObj, PROP_INTENTION_SONG) && isBlankStrProp(intentionObj, PROP_INTENTION_ARTIST) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_ALBUM) && isBlankStrProp(intentionObj, PROP_INTENTION_TAG) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_KEYWORD)
  }

  def isOnlyNeg(intentionObj: JsonObject): Boolean = {
    isBlankOrNeg(getStrProp(intentionObj, PROP_INTENTION_SONG)) &&
      isBlankOrNeg(getStrProp(intentionObj, PROP_INTENTION_ARTIST)) &&
      isBlankOrNeg(getStrProp(intentionObj, PROP_INTENTION_ALBUM)) &&
      isBlankOrNeg(getStrProp(intentionObj, PROP_INTENTION_TAG)) &&
      isBlankOrNeg(getStrProp(intentionObj, PROP_INTENTION_KEYWORD))
  }

  def isSearch(intentionJson: JsonObject): Boolean = {
    if (isBlankStrProp(intentionJson, PROP_INTENTION_SONG) && isBlankStrProp(intentionJson, PROP_INTENTION_ARTIST) &&
      isBlankStrProp(intentionJson, PROP_INTENTION_ALBUM) && isBlankStrProp(intentionJson, PROP_INTENTION_TAG) &&
      isBlankStrProp(intentionJson, PROP_INTENTION_KEYWORD)) {
      return false
    }
    //    if (isBlankStrProp(intentionJson, PROP_INTENTION_SONG) && isBlankStrProp(intentionJson, PROP_INTENTION_ARTIST) &&
    //      isBlankStrProp(intentionJson, PROP_INTENTION_ALBUM) && isNotBlankStrProp(intentionJson, PROP_INTENTION_TAG) &&
    //      isBlankStrProp(intentionJson, PROP_INTENTION_KEYWORD)) {
    //      return false
    //    }
    true
  }

  def isSearchExceptTag(intention: String): Boolean = {
    isSearchExceptTag(getJsonObject(intention))
  }

  def isSearchExceptTag(iObj: JsonObject): Boolean = {
    //iObj stands for intention json object.
    if (iObj == null) {
      return false
    }
    if (isBlankStrProp(iObj, PROP_INTENTION_SONG) && isBlankStrProp(iObj, PROP_INTENTION_ARTIST) &&
      isBlankStrProp(iObj, PROP_INTENTION_ALBUM) && isBlankStrProp(iObj, PROP_INTENTION_TAG) &&
      isBlankStrProp(iObj, PROP_INTENTION_KEYWORD)) {
      return false
    }
    if (isBlankStrProp(iObj, PROP_INTENTION_SONG) && isBlankStrProp(iObj, PROP_INTENTION_ARTIST) &&
      isBlankStrProp(iObj, PROP_INTENTION_ALBUM) && isNotBlankStrProp(iObj, PROP_INTENTION_TAG) &&
      isBlankStrProp(iObj, PROP_INTENTION_KEYWORD)) {
      return false
    }
    true
  }

  def getSearchPerms(intention: String): String = {
    getSearchPerms(getJsonObject(intention))
  }

  def getSearchPerms(iObj: JsonObject): String = {
    //perms stands for permutations
    if (iObj == null) {
      return "empty"
    }
    val sBuilder = new StringBuilder
    if (isNotBlankStrProp(iObj, PROP_INTENTION_SONG)) {
      sBuilder.append(PROP_INTENTION_SONG)
    }
    if (isNotBlankStrProp(iObj, PROP_INTENTION_ARTIST)) {
      if (sBuilder.nonEmpty) {
        sBuilder.append('+')
      }
      sBuilder.append(PROP_INTENTION_ARTIST)
    }
    if (isNotBlankStrProp(iObj, PROP_INTENTION_ALBUM)) {
      if (sBuilder.nonEmpty) {
        sBuilder.append('+')
      }
      sBuilder.append(PROP_INTENTION_ALBUM)
    }
    if (isNotBlankStrProp(iObj, PROP_INTENTION_TAG)) {
      if (sBuilder.nonEmpty) {
        sBuilder.append('+')
      }
      sBuilder.append(PROP_INTENTION_TAG)
    }
    if (isNotBlankStrProp(iObj, PROP_INTENTION_KEYWORD)) {
      if (sBuilder.nonEmpty) {
        sBuilder.append('+')
      }
      sBuilder.append(PROP_INTENTION_KEYWORD)
    }
    sBuilder.toString()
  }

  def getSearchTypeSimple(intentionJson: JsonObject): String = {
    if (isNotBlankStrProp(intentionJson, PROP_INTENTION_SONG) &&
      isBlankStrProp(intentionJson, PROP_INTENTION_ALBUM) && isBlankStrProp(intentionJson, PROP_INTENTION_TAG)) {
      return "song"
    }
    if (isBlankStrProp(intentionJson, PROP_INTENTION_SONG) && isNotBlankStrProp(intentionJson, PROP_INTENTION_ARTIST) &&
      isBlankStrProp(intentionJson, PROP_INTENTION_ALBUM) && isBlankStrProp(intentionJson, PROP_INTENTION_TAG)) {
      return "artist"
    }
    if (isBlankStrProp(intentionJson, PROP_INTENTION_SONG) && isBlankStrProp(intentionJson, PROP_INTENTION_ARTIST) &&
      isNotBlankStrProp(intentionJson, PROP_INTENTION_ALBUM) && isBlankStrProp(intentionJson, PROP_INTENTION_TAG)) {
      return "album"
    }
    "other"
  }

  def isSongOrArtistSearch(logIntentionJsonObject: JsonObject): Boolean = {
    if (logIntentionJsonObject == null) {
      return false
    }
    (isNormalSlot(getStrProp(logIntentionJsonObject, PROP_INTENTION_SONG)) ||
      isNormalSlot(getStrProp(logIntentionJsonObject, PROP_INTENTION_ARTIST))) &&
      isBlankStrProp(logIntentionJsonObject, PROP_INTENTION_ALBUM) &&
      isBlankStrProp(logIntentionJsonObject, PROP_INTENTION_TAG)
  }

  def isArtistSearch(intentionObj: JsonObject): Boolean = {
    if (intentionObj == null) {
      return false
    }
    val artistSlot = getStrProp(intentionObj, PROP_INTENTION_ARTIST)
    if (isNormalSlot(artistSlot)) {
      return isBlankStrProp(intentionObj, PROP_INTENTION_SONG) &&
        isBlankStrProp(intentionObj, PROP_INTENTION_ALBUM) &&
        isBlankStrProp(intentionObj, PROP_INTENTION_TAG)
    }
    false
  }

  def isSongSearch(iObj: JsonObject): Boolean = {
    //iObj stands for intention json object
    if (iObj == null) {
      return false
    }
    isNotBlankStrProp(iObj, PROP_INTENTION_SONG) &&
      isBlankStrProp(iObj, PROP_INTENTION_ARTIST) &&
      isBlankStrProp(iObj, PROP_INTENTION_ALBUM) &&
      isBlankStrProp(iObj, PROP_INTENTION_TAG)
  }

  def isSingleSlot(intentionObj: JsonObject): Boolean = {
    if (intentionObj == null) {
      return false
    }
    var nonEmptyCount = 0
    nonEmptyCount = nonEmptyCount + (if (isNormalSlot(getStrProp(intentionObj, PROP_INTENTION_SONG))) 1 else 0)
    nonEmptyCount = nonEmptyCount + (if (isNormalSlot(getStrProp(intentionObj, PROP_INTENTION_ARTIST))) 1 else 0)
    nonEmptyCount = nonEmptyCount + (if (isNormalSlot(getStrProp(intentionObj, PROP_INTENTION_ALBUM))) 1 else 0)
    nonEmptyCount = nonEmptyCount + (if (isNormalSlot(getStrProp(intentionObj, PROP_INTENTION_TAG))) 1 else 0)
    if (nonEmptyCount == 1) {
      return true
    }
    false
  }

  def isSongArtistSearch(intention: String): Boolean = {
    isSongArtistSearch(getJsonObject(intention))
  }

  def isSongArtistSearch(logIntentionJsonObject: JsonObject): Boolean = {
    if (logIntentionJsonObject == null) {
      return false
    }
    isNormalSlot(getStrProp(logIntentionJsonObject, PROP_INTENTION_SONG)) &&
      isNormalSlot(getStrProp(logIntentionJsonObject, PROP_INTENTION_ARTIST)) &&
      isBlankStrProp(logIntentionJsonObject, PROP_INTENTION_ALBUM) &&
      isBlankStrProp(logIntentionJsonObject, PROP_INTENTION_TAG)
  }

  def isSongTagSearch(intention: String): Boolean = {
    isSongTagSearch(getJsonObject(intention))
  }

  def isSongTagSearch(logIntentionJsonObject: JsonObject): Boolean = {
    if (logIntentionJsonObject == null) {
      return false
    }
    isNormalSlot(getStrProp(logIntentionJsonObject, PROP_INTENTION_SONG)) &&
      isNormalSlot(getStrProp(logIntentionJsonObject, PROP_INTENTION_TAG)) &&
      isBlankStrProp(logIntentionJsonObject, PROP_INTENTION_ALBUM) &&
      isBlankStrProp(logIntentionJsonObject, PROP_INTENTION_ARTIST)
  }

  def isAlbumSearch(intentionObj: JsonObject): Boolean = {
    if (intentionObj == null) {
      return false
    }
    isNormalSlot(getStrProp(intentionObj, PROP_INTENTION_ALBUM)) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_ARTIST) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_SONG) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_TAG)
  }

  def isTagSearch(intention: String): Boolean = {
    isTagSearch(getJsonObject(intention))
  }

  def isTagSearch(intentionObj: JsonObject): Boolean = {
    if (intentionObj == null) {
      return false
    }
    isNotBlankStrProp(intentionObj, PROP_INTENTION_TAG) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_ARTIST) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_SONG) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_ALBUM)
  }

  def isKeywordSearch(intentionObj: JsonObject): Boolean = {
    if (intentionObj == null) {
      return false
    }
    isNotBlankStrProp(intentionObj, PROP_INTENTION_KEYWORD) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_ARTIST) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_SONG) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_ALBUM) &&
      isBlankStrProp(intentionObj, PROP_INTENTION_TAG)
  }

  def isLyricSearch(intentionObj: JsonObject): Boolean = {
    if (intentionObj == null) {
      return false
    }
    val func = getStrProp(intentionObj, PROP_INTENTION_FUNC)
    StringUtils.isNotBlank(func) && func.contains("lyric_search")
  }

  def isEdgeSearch(intentionObj: JsonObject): Boolean = {
    !isSongSearch(intentionObj) && !isSongArtistSearch(intentionObj) && !isAlbumSearch(intentionObj) &&
      !isArtistSearch(intentionObj) && !isTagSearch(intentionObj)
  }

  def isAlternatingV2Search(intentObj: JsonObject): Boolean = {
    val song = getStrProp(intentObj, PROP_INTENTION_SONG)
    val artist = getStrProp(intentObj, PROP_INTENTION_ARTIST)
    val album = getStrProp(intentObj, PROP_INTENTION_ALBUM)
    val tag = getStrProp(intentObj, PROP_INTENTION_TAG)
    StringUtils.isNotBlank(song) && (isNeg(artist) || isNeg(album) || isNeg(tag)) ||
      StringUtils.isNotBlank(artist) && (isNeg(song) || isNonBlankOrNeg(album) || isNonBlankOrNeg(tag))
  }

  def isSongOptSearch(intentObj: JsonObject): Boolean = {
    if (intentObj == null) {
      return false
    }
    val optInt = getStrProp(intentObj, PROP_INTENTION_OPT_INTENTION)
    isSongSearch(intentObj) && StringUtils.isNotBlank(optInt) && optInt.contains(PROP_INTENTION_TAG)
  }

  def isSongOptSearch(intention: String): Boolean = {
    isSongOptSearch(getJsonObject(intention))
  }

  def isSearchRequest(log: AiContentFrontBackLog): Boolean = {
    if (log.getPlayInfoLog == null || log.getPlayInfoLog.size() == 0) {
      return false
    }
    log.getPlayInfoLog.asScala.foreach(pi => {
      if (pi.getRefer == REFER_SEARCH) {
        return true
      }
    })
    false
  }

  def isSearchResult(cObj: JsonObject): Boolean = {
    if (cObj == null) {
      return false
    }
    StringUtils.equalsIgnoreCase(getRefer(cObj), REFER_SEARCH)
  }

  def isQQGreenVip(content: String): Boolean = {
    if (StringUtils.isBlank(content)) {
      return false
    }
    val cObj = getJsonObject(content)
    if (hasProp(cObj, PROP_CONTENT_MEMBER_LEVEL)) {
      if (getStrProp(cObj, PROP_CONTENT_MEMBER_LEVEL) == MEMBER_LEVEL_QQ_GREEN) {
        return true
      }
    }
    false
  }

  def isQQQuery(jsonObject: JsonObject): Boolean = {
    if (!hasProp(jsonObject, PROP_CONTENT_SOURCE)) {
      return false
    }
    val source = getStrProp(jsonObject, PROP_CONTENT_SOURCE)
    source == QQ
  }

  def generateSlotInfo(jsonObject: JsonObject): String = {
    //返回 song##artist##album##tag##keyword
    val song = getStrProp(jsonObject, PROP_INTENTION_SONG)
    val artist = getStrProp(jsonObject, PROP_INTENTION_ARTIST)
    val album = getStrProp(jsonObject, PROP_INTENTION_ALBUM)
    val tag = getStrProp(jsonObject, PROP_INTENTION_TAG)
    val keyword = getStrProp(jsonObject, PROP_INTENTION_KEYWORD)
    getSlotOrDefault(song) + INTENTION_SLOT_SEP + getSlotOrDefault(artist) + INTENTION_SLOT_SEP +
      getSlotOrDefault(album) + INTENTION_SLOT_SEP + getSlotOrDefault(tag) + INTENTION_SLOT_SEP +
      getSlotOrDefault(keyword)
  }

  def generateIntentionType(jsonObject: JsonObject): String = {
    //返回 song##artist##album##tag##keyword
    val song = getStrProp(jsonObject, PROP_INTENTION_SONG)
    val artist = getStrProp(jsonObject, PROP_INTENTION_ARTIST)
    val album = getStrProp(jsonObject, PROP_INTENTION_ALBUM)
    val tag = getStrProp(jsonObject, PROP_INTENTION_TAG)
    val keyword = getStrProp(jsonObject, PROP_INTENTION_KEYWORD)
    getSlotType(song) + INTENTION_SLOT_TYPE_SEP + getSlotType(artist) + INTENTION_SLOT_TYPE_SEP +
      getSlotType(album) + INTENTION_SLOT_TYPE_SEP + getSlotType(tag) + INTENTION_SLOT_TYPE_SEP +
      getSlotType(keyword)
  }

  def isArtistSearchBySlotInfo(slotInfo: String, sep: String = INTENTION_SLOT_SEP): Boolean = {
    val slots = slotInfo.split(sep)
    val artist = slots(1)
    StringUtils.equals(slots(0), INTENTION_SLOT_EMPTY) &&
      StringUtils.equals(slots(2), INTENTION_SLOT_EMPTY) &&
      StringUtils.equals(slots(3), INTENTION_SLOT_EMPTY) &&
      StringUtils.equals(slots(4), INTENTION_SLOT_EMPTY) &&
      !StringUtils.equals(artist, INTENTION_SLOT_EMPTY) && isNormalSlot(artist)
  }

  def getFromSlotInfo(slotInfo: String, sep: String = INTENTION_SLOT_SEP, index: Int): String = {
    val slots = slotInfo.split(sep)
    if (index >= slots.length) {
      return StringUtils.EMPTY
    }
    slots(index)
  }

  def generateSlotInfo(jsonObject: JsonObject, sep: String): String = {
    //返回 song##artist##album##tag##keyword
    val song = getStrProp(jsonObject, PROP_INTENTION_SONG)
    val artist = getStrProp(jsonObject, PROP_INTENTION_ARTIST)
    val album = getStrProp(jsonObject, PROP_INTENTION_ALBUM)
    val tag = getStrProp(jsonObject, PROP_INTENTION_TAG)
    val keyword = getStrProp(jsonObject, PROP_INTENTION_KEYWORD)
    getSlotOrDefault(song) + sep + getSlotOrDefault(artist) + sep +
      getSlotOrDefault(album) + sep + getSlotOrDefault(tag) + sep +
      getSlotOrDefault(keyword)
  }

  def generateQuerySlotInfo(jsonObject: JsonObject): String = {
    //返回 song##artist##album##tag##keyword
    val query = getStrProp(jsonObject, PROP_INTENTION_QUERY)
    val song = getStrProp(jsonObject, PROP_INTENTION_SONG)
    val artist = getStrProp(jsonObject, PROP_INTENTION_ARTIST)
    val album = getStrProp(jsonObject, PROP_INTENTION_ALBUM)
    val tag = getStrProp(jsonObject, PROP_INTENTION_TAG)
    val keyword = getStrProp(jsonObject, PROP_INTENTION_KEYWORD)
    getSlotOrDefault(query) + INTENTION_SLOT_SEP + getSlotOrDefault(song) + INTENTION_SLOT_SEP + getSlotOrDefault(artist) + INTENTION_SLOT_SEP +
      getSlotOrDefault(album) + INTENTION_SLOT_SEP + getSlotOrDefault(tag) + INTENTION_SLOT_SEP +
      getSlotOrDefault(keyword)
  }

  def getSlotInfo(intention: String, searchType: String): String = {
    if (searchType == PROP_INTENTION_SONG) {
      val intentionObject = getJsonObject(intention)
      if (isSongSearch(intentionObject)) {
        return generateSlotInfo(intentionObject)
      }
    } else if (searchType == PROP_INTENTION_ARTIST) {
      val intentionObject = getJsonObject(intention)
      if (isArtistSearch(intentionObject)) {
        return generateSlotInfo(intentionObject)
      }
    } else if (searchType == PROP_INTENTION_SONG + PROP_INTENTION_ARTIST) {
      val intentionObject = getJsonObject(intention)
      if (isSongArtistSearch(intentionObject)) {
        return generateSlotInfo(intentionObject)
      }
    }
    null
  }

  def getFirstMusic(contentJson: JsonObject): JsonObject = {
    if (contentJson == null ||
      !hasJsonArrayProp(contentJson, PROP_CONTENT_MUSICS) && !hasJsonArrayProp(contentJson, PROP_CONTENT_DIRECTIVE)) {
      return null
    }
    val musicJArray = getJsonArrayProp(contentJson, PROP_CONTENT_MUSICS)
    if (musicJArray != null && musicJArray.size() > 0) {
      return musicJArray.get(0).getAsJsonObject
    }
    if (contentJson.has(PROP_CONTENT_DIRECTIVE) && contentJson.get(PROP_CONTENT_DIRECTIVE).isJsonArray) {
      val directiveJArray = contentJson.get(PROP_CONTENT_DIRECTIVE).getAsJsonArray
      if (directiveJArray != null && directiveJArray.size() > 0) {
        val directiveContentJson = directiveJArray.get(0).getAsJsonObject
        if (directiveContentJson != null && directiveContentJson.has(PROP_DIRECTIVE_ITEMS)) {
          val itemJArray = directiveContentJson.get(PROP_DIRECTIVE_ITEMS).getAsJsonArray
          if (itemJArray != null && itemJArray.size() > 0) {
            return itemJArray.get(0).getAsJsonObject
          }
        }
      }
    }
    null
  }

  def getMusicObj(cObj: JsonObject, index: Int = 0): JsonObject = {
    val musicArr = getMusicArray(cObj)
    if (isEmpty(musicArr) || index >= musicArr.size()) {
      return null
    }
    musicArr.get(index).getAsJsonObject
  }

  def getMusicArray(content: String): JsonArray = {
    getMusicArray(getJsonObject(content))
  }

  def getMusicArray(cObj: JsonObject): JsonArray = {
    if (cObj == null || !cObj.has(PROP_CONTENT_MUSICS) && !cObj.has(PROP_CONTENT_DIRECTIVE)) {
      return null
    }
    if (cObj.has(PROP_CONTENT_MUSICS)) {
      val musicArray = getJsonArrayProp(cObj, PROP_CONTENT_MUSICS)
      if (isNotEmpty(musicArray)) {
        return musicArray
      }
    }
    if (cObj.has(PROP_CONTENT_DIRECTIVE)) {
      val directiveArray = getJsonArrayProp(cObj, PROP_CONTENT_DIRECTIVE)
      if (isNotEmpty(directiveArray)) {
        val directiveObj = directiveArray.get(0).getAsJsonObject
        if (directiveObj != null && directiveObj.has(PROP_DIRECTIVE_ITEMS)) {
          val itemArray = getJsonArrayProp(cObj, PROP_DIRECTIVE_ITEMS)
          if (isNotEmpty(itemArray)) {
            return itemArray
          }
        }
      }
    }
    null
  }

  def getCertainMusic(contentJson: JsonObject, index: Int): JsonObject = {
    if (contentJson == null) {
      return null
    }
    val musicJArray = getJsonArrayProp(contentJson, PROP_CONTENT_MUSICS)
    if (musicJArray != null && musicJArray.size() > index) {
      return musicJArray.get(index).getAsJsonObject
    }
    val directiveJArray = getJsonArrayProp(contentJson, PROP_CONTENT_DIRECTIVE)
    if (isNotEmpty(directiveJArray)) {
      val directiveContentJson = directiveJArray.get(0).getAsJsonObject
      if (directiveContentJson != null && directiveContentJson.has(PROP_DIRECTIVE_ITEMS)) {
        val itemJArray = directiveContentJson.get(PROP_DIRECTIVE_ITEMS).getAsJsonArray
        if (itemJArray != null && itemJArray.size() > index) {
          return itemJArray.get(index).getAsJsonObject
        }
      }
    }
    null
  }

  def getSearchType(intention: String): String = {
    if (StringUtils.isBlank(intention)) {
      return null
    }
    val intentionJson = getJsonObject(intention)
    if (intentionJson == null) {
      null
    }
    getSearchType(intentionJson)
  }

  def getSearchType(jsonObject: JsonObject): String = {
    //返回 song##artist##album##tag##keyword
    val song = getStrProp(jsonObject, PROP_INTENTION_SONG)
    val artist = getStrProp(jsonObject, PROP_INTENTION_ARTIST)
    val album = getStrProp(jsonObject, PROP_INTENTION_ALBUM)
    val tag = getStrProp(jsonObject, PROP_INTENTION_TAG)
    val keyword = getStrProp(jsonObject, PROP_INTENTION_KEYWORD)
    if (StringUtils.isBlank(song) && StringUtils.isBlank(artist) && StringUtils.isBlank(album) &&
      StringUtils.isBlank(tag) && StringUtils.isBlank(keyword)) {
      return null
    }
    if (StringUtils.isBlank(song) && StringUtils.isBlank(artist) && StringUtils.isBlank(album) &&
      StringUtils.isBlank(tag) && StringUtils.isNotBlank(keyword)) {
      return PROP_INTENTION_KEYWORD
    }
    if (StringUtils.isBlank(tag) && StringUtils.isBlank(album)) {
      if (StringUtils.isNotBlank(song) && StringUtils.isNotBlank(artist)) {
        return "song_artist"
      } else if (StringUtils.isNotBlank(song)) {
        return PROP_INTENTION_SONG
      } else if (StringUtils.isNotBlank(artist)) {
        return PROP_INTENTION_ARTIST
      }
    }
    if (StringUtils.isBlank(song) && StringUtils.isBlank(artist)) {
      if (StringUtils.isNotBlank(tag) && StringUtils.isBlank(album)) {
        return PROP_INTENTION_TAG
      }
      if (StringUtils.isNotBlank(album) && StringUtils.isBlank(tag)) {
        return PROP_INTENTION_ALBUM
      }
    }
    "other"
  }

  def getCertainMusic(contentObj: JsonObject, id: String): JsonObject = {
    if (contentObj == null || StringUtils.isBlank(id)) {
      return null
    }
    val musicArray = contentObj.get(PROP_CONTENT_MUSICS).getAsJsonArray
    if (isNotEmpty(musicArray)) {
      for (i <- 0 until musicArray.size()) {
        val curObj = musicArray.get(i).getAsJsonObject
        if (id.equals(getMusicProp(curObj, PROP_MUSIC_ID))) {
          return curObj
        }
      }
    }
    val directiveArray = getJsonArrayProp(contentObj, PROP_CONTENT_DIRECTIVE)
    if (isNotEmpty(directiveArray)) {
      val directiveObj = directiveArray.get(0).getAsJsonObject
      if (directiveObj != null) {
        val itemArray = getJsonArrayProp(directiveObj, PROP_DIRECTIVE_ITEMS)
        if (isNotEmpty(itemArray)) {
          for (i <- 0 until itemArray.size()) {
            val curObj = itemArray.get(i).getAsJsonObject
            if (id.equals(getMusicProp(curObj, PROP_MUSIC_ID))) {
              return curObj
            }
          }
        }
      }
    }
    null
  }

  def getCertainMusic(array: JsonArray, id: String): JsonObject = {
    if (isEmpty(array) || StringUtils.isBlank(id)) {
      return null
    }
    for (i <- 0 until array.size()) {
      val curObj = array.get(i).getAsJsonObject
      if (id.equals(getMusicProp(curObj, PROP_MUSIC_ID))) {
        return curObj
      }
    }
    null
  }

  def getMusics(content: String): JsonArray = {
    getMusics(getJsonObject(content))
  }

  def getMusics(contentObj: JsonObject): JsonArray = {
    if (contentObj == null) {
      return null
    }
    val musicArray = getJsonArrayProp(contentObj, PROP_CONTENT_MUSICS)
    if (isNotEmpty(musicArray)) {
      return musicArray
    }
    val directiveArray = getJsonArrayProp(contentObj, PROP_CONTENT_DIRECTIVE)
    if (isNotEmpty(directiveArray)) {
      val directiveObj = directiveArray.get(0).getAsJsonObject
      if (directiveObj != null) {
        val itemArray = getJsonArrayProp(directiveObj, PROP_DIRECTIVE_ITEMS)
        if (isNotEmpty(itemArray)) {
          return itemArray
        }
      }
    }
    null
  }

  def hasMarkInfo(jsonObject: JsonObject, mark: String): Boolean = {
    if (jsonObject == null || StringUtils.isEmpty(mark)) {
      return false
    }
    val markInfo = getMarkInfo(jsonObject)
    StringUtils.isNotBlank(markInfo) && markInfo.contains(mark)
  }

  def getMarkInfo(jsonObject: JsonObject): String = {
    if (jsonObject == null) {
      return StringUtils.EMPTY
    }
    if (hasProp(jsonObject, PROP_MUSIC_MARK_INFO)) {
      return getStrProp(jsonObject, PROP_MUSIC_MARK_INFO)
    }
    if (hasJsonObjectProp(jsonObject, PROP_DIRECTIVE_EXTEND)) {
      val extendJson = getJsonObjectProp(jsonObject, PROP_DIRECTIVE_EXTEND)
      if (hasProp(extendJson, PROP_MUSIC_MARK_INFO)) {
        return getStrProp(extendJson, PROP_MUSIC_MARK_INFO)
      }
    }
    StringUtils.EMPTY
  }

  def getMusicProp(jsonObject: JsonObject, prop: String): String = {
    if (jsonObject == null) {
      return StringUtils.EMPTY
    }
    if (hasProp(jsonObject, prop)) {
      return getStrProp(jsonObject, prop)
    }
    if (hasJsonObjectProp(jsonObject, PROP_DIRECTIVE_EXTEND)) {
      val extendObj = getJsonObjectProp(jsonObject, PROP_DIRECTIVE_EXTEND)
      if (hasProp(extendObj, prop)) {
        return getStrProp(extendObj, prop)
      }
    }
    StringUtils.EMPTY
  }

  def getMusicBoolProp(jsonObject: JsonObject, prop: String): Boolean = {
    if (jsonObject == null) {
      return false
    }
    if (hasProp(jsonObject, prop)) {
      return getBooleanProp(jsonObject, prop)
    }
    if (hasJsonObjectProp(jsonObject, PROP_DIRECTIVE_EXTEND)) {
      val extendObj = getJsonObjectProp(jsonObject, PROP_DIRECTIVE_EXTEND)
      if (hasProp(extendObj, prop)) {
        return getBooleanProp(extendObj, prop)
      }
    }
    false
  }

  def getMusicProp(jsonObject: JsonObject, prop1: String, prop2: String): String = {
    val property1 = getMusicProp(jsonObject, prop1)
    if (StringUtils.isBlank(property1)) {
      return getMusicProp(jsonObject, prop2)
    }
    property1
  }

  def getGlobalId(jsonObject: JsonObject): String = {
    if (jsonObject == null) {
      return StringUtils.EMPTY
    }
    if (hasProp(jsonObject, PROP_MUSIC_GLOBAL_ID)) {
      return getStrProp(jsonObject, PROP_MUSIC_GLOBAL_ID)
    }
    if (hasJsonArrayProp(jsonObject, PROP_DIRECTIVE_EXTEND)) {
      val extendJson = getJsonObjectProp(jsonObject, PROP_DIRECTIVE_EXTEND)
      if (hasProp(extendJson, PROP_MUSIC_GLOBAL_ID)) {
        return getStrProp(extendJson, PROP_MUSIC_GLOBAL_ID)
      }
    }
    StringUtils.EMPTY
  }

  def getAsMusicInfo(musicObject: JsonObject): String = {
    var ret: String = StringUtils.EMPTY
    if (hasProp(musicObject, PROP_MUSIC_SONG)) {
      ret += getStrProp(musicObject, PROP_MUSIC_SONG) + MUSIC_FIELD_SEP
    }
    if (hasProp(musicObject, PROP_MUSIC_ARTIST)) {
      ret += getStrProp(musicObject, PROP_MUSIC_ARTIST) + MUSIC_FIELD_SEP
    }
    if (hasProp(musicObject, PROP_MUSIC_ALBUM_NAME)) {
      ret += getStrProp(musicObject, PROP_MUSIC_ALBUM_NAME) + MUSIC_FIELD_SEP
    }
    if (hasProp(musicObject, PROP_MUSIC_ID)) {
      ret += getStrProp(musicObject, PROP_MUSIC_ID) + MUSIC_FIELD_SEP
    }
    if (hasProp(musicObject, PROP_MUSIC_GLOBAL_ID)) {
      ret += getStrProp(musicObject, PROP_MUSIC_GLOBAL_ID) + MUSIC_FIELD_SEP
    }
    ret
  }

  def getEid(content: String): String = {
    val cObj = getJsonObject(content)
    getEid(cObj)
  }

  def getEid(cObj: JsonObject): String = {
    if (cObj == null || !cObj.has(PROP_CONTENT_MUSICS) && !cObj.has(PROP_CONTENT_DIRECTIVE)) {
      return null
    }
    if (cObj.has(PROP_CONTENT_MUSICS) && cObj.get(PROP_CONTENT_MUSICS).isJsonArray) {
      val musicJArray = cObj.get(PROP_CONTENT_MUSICS).getAsJsonArray
      if (musicJArray != null && musicJArray.size() > 0) {
        return getEidFromJsonObject(musicJArray.get(0).getAsJsonObject)
      }
    }
    if (cObj.has(PROP_CONTENT_DIRECTIVE) && cObj.get(PROP_CONTENT_DIRECTIVE).isJsonArray) {
      val directiveJArray = cObj.get(PROP_CONTENT_DIRECTIVE).getAsJsonArray
      if (directiveJArray != null && directiveJArray.size() > 0) {
        val directiveContentJson = directiveJArray.get(0).getAsJsonObject
        if (directiveContentJson != null && directiveContentJson.has(PROP_DIRECTIVE_ITEMS)) {
          val itemJArray = directiveContentJson.get(PROP_DIRECTIVE_ITEMS).getAsJsonArray
          if (itemJArray != null && itemJArray.size() > 0) {
            val itemJson = itemJArray.get(0).getAsJsonObject
            if (itemJson != null && itemJson.has(PROP_DIRECTIVE_EXTEND) &&
              itemJson.get(PROP_DIRECTIVE_EXTEND).isJsonObject) {
              return getEidFromJsonObject(itemJson.get(PROP_DIRECTIVE_EXTEND).getAsJsonObject)
            }
          }
        }
      }
    }
    null
  }

  def getEidFromMusic(musicObj: JsonObject): String = {
    if (musicObj == null) {
      return null
    }
    val eid = getEidFromJsonObject(musicObj)
    if (StringUtils.isNotBlank(eid)) {
      return eid
    }
    if (musicObj != null && musicObj.has(PROP_DIRECTIVE_EXTEND) &&
      musicObj.get(PROP_DIRECTIVE_EXTEND).isJsonObject) {
      return getEidFromJsonObject(musicObj.get(PROP_DIRECTIVE_EXTEND).getAsJsonObject)
    }
    null
  }

  def getCp(cObj: JsonObject): String = {
    val firstObj = getMusicObj(cObj)
    if (firstObj != null) {
      if (isDirectiveContentMusic(firstObj)) {
        return getMusicProp(firstObj, PROP_ITEM_CP)
      } else {
        return getMusicProp(firstObj, PROP_MUSIC_ORIGIN)
      }
    }
    StringUtils.EMPTY
  }

  def getRefer(cObj: JsonObject): String = {
    val firstObj = getMusicObj(cObj)
    if (firstObj != null) {
      return getStrProp(firstObj, PROP_MUSIC_REFER)
    }
    StringUtils.EMPTY
  }

  def isFirstContentMv(contenJson: JsonObject): Boolean = {
    val firstContentMusic = getFirstContentMusic(contenJson)
    if (firstContentMusic != null) {
      val id = getStrProp(firstContentMusic, PROP_MUSIC_ID)
      return id.startsWith(MV_PREFIX)
    }
    false
  }

  def isFirstContentVip(contentJson: JsonObject): Boolean = {
    val firstContentMusic = getFirstContentMusic(contentJson)
    if (firstContentMusic != null) {
      if (isDirectiveContentMusic(firstContentMusic)) {
        if (hasJsonObjectProp(firstContentMusic, PROP_DIRECTIVE_EXTEND)) {
          val extendJson = getJsonObjectProp(firstContentMusic, PROP_DIRECTIVE_EXTEND)
          return getBooleanProp(extendJson, PROP_MUSIC_IS_VIP)
        }
      } else {
        return getBooleanProp(firstContentMusic, PROP_MUSIC_IS_VIP)
      }
    }
    false
  }

  def getFirstContentMusic(contentJson: JsonObject): JsonObject = {
    if (contentJson == null || !contentJson.has(PROP_CONTENT_MUSICS) && !contentJson.has(PROP_CONTENT_DIRECTIVE)) {
      return null
    }
    if (contentJson.has(PROP_CONTENT_MUSICS) && contentJson.get(PROP_CONTENT_MUSICS).isJsonArray) {
      val musicJArray = contentJson.get(PROP_CONTENT_MUSICS).getAsJsonArray
      if (musicJArray != null && musicJArray.size() > 0) {
        return musicJArray.get(0).getAsJsonObject
      }
    }
    if (contentJson.has(PROP_CONTENT_DIRECTIVE) && contentJson.get(PROP_CONTENT_DIRECTIVE).isJsonArray) {
      val directiveJArray = contentJson.get(PROP_CONTENT_DIRECTIVE).getAsJsonArray
      if (directiveJArray != null && directiveJArray.size() > 0) {
        val directiveContentJson = directiveJArray.get(0).getAsJsonObject
        if (directiveContentJson != null && directiveContentJson.has(PROP_DIRECTIVE_ITEMS)) {
          val itemJArray = directiveContentJson.get(PROP_DIRECTIVE_ITEMS).getAsJsonArray
          if (itemJArray != null && itemJArray.size() > 0) {
            return itemJArray.get(0).getAsJsonObject
          }
        }
      }
    }
    null
  }

  def isDirectiveContentMusic(jsonObject: JsonObject): Boolean = {
    StringUtils.isNotBlank(getStrProp(jsonObject, PROP_DIRECTIVE_EXTEND))
  }

  def getSongSlot(jsonObject: JsonObject): String = {
    if (!jsonObject.has(PROP_INTENTION_SONG)) {
      return null
    }
    getStrProp(jsonObject, PROP_INTENTION_SONG)
  }

  def getRequestPlayStats(log: AiContentFrontBackLog): (Long, Long) = {
    var isPlayFirst = false
    var isFinishFirst = false
    log.getPlayInfoLog.asScala.foreach(info => {
      if (info.getOffset == 0 && info.getRefer == "search") {
        isPlayFirst = true
        if (info.getSwitchtype == PLAY_INFO_AUTOSWITCH) {
          isFinishFirst = true
        }
      }
    })
    (if (isPlayFirst) 1L else 0L, if (isFinishFirst) 1L else 0L)
  }

  def getRequestPlayStatsNonEmpty(log: AiContentFrontBackLog): (Long, Long) = {
    var isPlayFirst = false
    var isFinishFirst = false
    if (CollectionUtils.isNotEmpty(log.getPlayInfoLog)) {
      isPlayFirst = true
    }
    log.getPlayInfoLog.asScala.foreach(info => {
      if (info.getOffset == 0 && info.getRefer == "search") {
        isPlayFirst = true
        if (info.getSwitchtype == PLAY_INFO_AUTOSWITCH) {
          isFinishFirst = true
        }
      }
    })
    (if (isPlayFirst) 1L else 0L, if (isFinishFirst) 1L else 0L)
  }

  def getStatsPerQuery(log: AiContentFrontBackLog): (Long, Long) = {
    var isFinishFirst = false
    if (CollectionUtils.isEmpty(log.getPlayInfoLog)) {
      return (1L, 0L)
    }
    log.getPlayInfoLog.asScala.foreach(info => {
      if (info.getOffset == 0 && info.getRefer == "search") {
        if (info.getSwitchtype == PLAY_INFO_AUTOSWITCH) {
          isFinishFirst = true
        }
      }
    })
    (1L, if (isFinishFirst) 1L else 0L)
  }

  def getLess10Stats(log: AiContentFrontBackLog): (Long, Long) = {
    var played = false
    var time1st = 0L
    if (CollectionUtils.isNotEmpty(log.getPlayInfoLog)) {
      played = true
    }
    log.getPlayInfoLog.asScala.foreach(info => {
      if (info.getOffset == 0 && info.getRefer == "search") {
        time1st += info.getEndtime - info.getStartime
      }
    })
    (if (played) 1L else 0L, if (played && time1st <= 10) 1L else 0L)
  }

  def getFirstPlayId(log: AiContentFrontBackLog): String = {
    log.getPlayInfoLog.asScala.foreach(info => {
      if (info.getOffset == 0 && info.getRefer == "search") {
        return info.getResid
      }
    })
    StringUtils.EMPTY
  }

  def getRequestPlayTime(log: AiContentFrontBackLog, isSongSearch: Boolean = false): Long = {
    var sumTime = 0L
    var isSearch = false
    var isSureNotSearch = false
    if (log.getPlayInfoLog.size() == 0) {
      return -1L
    }
    log.getPlayInfoLog.asScala.sortWith(_.startime < _.startime).foreach(info => {
      if (info.getRefer == "search") {
        isSearch = true
      }
      if (isSongSearch && StringUtils.isNotBlank(info.getRefer) && info.getRefer.contains("Search")) {
        isSearch = true
      }
      if (StringUtils.isNotBlank(info.getRefer)) {
        if (isSongSearch && info.getRefer.contains("Search")) {
          isSearch = true
        }
        if (info.getRefer.contains("alarm") || info.getRefer.contains("qqSearchQuery")) {
          isSureNotSearch = true
        }
      }
      val time = info.getEndtime - info.getStartime
      if (time >= 0 && time < 600) {
        sumTime += time
      }
    })
    if (isSearch && !isSureNotSearch) (if (sumTime > 1800) 1800 else sumTime) else -1L
  }

  def getPlayTimePerQuery(log: AiContentFrontBackLog, isSongSearch: Boolean = false): Long = {
    var sumTime = 0L
    var isSearch = false
    var isSureNotSearch = false
    if (CollectionUtils.isEmpty(log.getPlayInfoLog)) {
      return 0L
    }
    log.getPlayInfoLog.asScala.sortWith(_.startime < _.startime).foreach(info => {
      if (info.getRefer == "search") {
        isSearch = true
      }
      if (isSongSearch && StringUtils.isNotBlank(info.getRefer) && info.getRefer.contains("Search")) {
        isSearch = true
      }
      if (StringUtils.isNotBlank(info.getRefer)) {
        if (isSongSearch && info.getRefer.contains("Search")) {
          isSearch = true
        }
        if (info.getRefer.contains("alarm") || info.getRefer.contains("qqSearchQuery")) {
          isSureNotSearch = true
        }
      }
      val time = info.getEndtime - info.getStartime
      if (time >= 0 && time < 600) {
        sumTime += time
      }
    })
    if (isSearch && !isSureNotSearch || CollectionUtils.isEmpty(log.getPlayInfoLog))
      (if (sumTime > 1800) 1800 else sumTime) else -1L
  }

  def isNormalSlot(slot: String): Boolean = {
    if (StringUtils.isBlank(slot)) {
      return false
    }
    !slot.matches(NON_NORMAL_SLOT_MARK)
  }

  def isNonBlankOrNeg(slot: String): Boolean = {
    StringUtils.isNotBlank(slot) || slot.contains(NEG_MARK)
  }

  def isNeg(slot: String): Boolean = {
    StringUtils.isNotBlank(slot) && slot.contains(NEG_MARK)
  }

  def isBlankOrNeg(slot: String): Boolean = {
    StringUtils.isBlank(slot) || slot.contains(NEG_MARK)
  }

  def getCpSongId(cp: String, id: String): String = {
    if (StringUtils.isBlank(cp) || StringUtils.isBlank(id)) {
      return StringUtils.EMPTY
    }
    if (id.startsWith("mv")) {
      return id
    }
    (if (cp.equalsIgnoreCase("miui")) "mi" else cp) + "_" + id
  }

  def getCpSongId(music: JsonObject): String = {
    if (music == null) {
      return StringUtils.EMPTY
    }
    val cp = getMusicProp(music, PROP_MUSIC_CP)
    val id = getMusicProp(music, PROP_MUSIC_ID)
    val markInfo = getMusicProp(music, PROP_MUSIC_MARK_INFO)
    if (StringUtils.isNotBlank(markInfo)) {
      val csi = markInfo.split("::")(0)
      if (StringUtils.isNotBlank(csi) && !csi.contains("qqResult")) {
        return csi
      }
    }
    getCpSongId(cp, id)
  }

  def getFirstAutoSwitchId(log: AiContentFrontBackLog): String = {
    if (log == null || CollectionUtils.isEmpty(log.getPlayInfoLog)) {
      return StringUtils.EMPTY
    }
    log.getPlayInfoLog.asScala.foreach(pi => {
      if (pi.getOffset == 0 && (PLAY_INFO_AUTOSWITCH.equalsIgnoreCase(pi.getSwitchtype) ||
        pi.getEndtime - pi.getStartime > 90)) {
        return pi.getResid
      }
    })

    StringUtils.EMPTY
  }

  def isRealUser(log: AiContentFrontBackLog): Boolean = {
    if (log == null || StringUtils.isBlank(log.getUid) || !StringUtils.isNumeric(log.getUid)) {
      return false
    }
    log.getUid.toLong > 10000L
  }

  def isRealMaskUser(log: AiContentFrontBackLog): Boolean = {
    if (log == null || StringUtils.isBlank(log.getMaskuid) || !StringUtils.isNumeric(log.getMaskuid)) {
      return false
    }
    log.getMaskuid.toLong > 10000L
  }

  def isValidGlobalId(gid: String): Boolean = {
    if (StringUtils.isBlank(gid) || !StringUtils.isNumeric(gid)) {
      return false
    }
    gid.toLong > 100000L
  }

  def calcPlayAndFinishCount(log: AiContentFrontBackLog): (Long, Long) = {
    if (log == null || CollectionUtils.isEmpty(log.getPlayInfoLog)) {
      return (0L, 0L)
    }
    val reduced = reducePlayInfo(log.getPlayInfoLog.asScala)
    (reduced.length, reduced.count(_._2._2))
  }

  def calcPlayAndSwitchCount(log: AiContentFrontBackLog): (Long, Long) = {
    if (log == null || CollectionUtils.isEmpty(log.getPlayInfoLog)) {
      return (0L, 0L)
    }
    val reduced = reducePlayInfoWithManSwitch(log.getPlayInfoLog.asScala)
    (reduced.length, reduced.count(_._2._2))
  }

  def getObservedEid(eid: String): String = {
    if (StringUtils.isBlank(eid) || !eid.contains(":::")) {
      return eid
    }
    eid.split(":::")(0)
  }

  def getSupportEid(eid: String): String = {
    if (StringUtils.isBlank(eid) || !eid.contains(":::")) {
      return "nonsupport"
    }
    eid.split(":::")(1)
  }

  def getVoiceMusicObj(cObj: JsonObject, index: Int): JsonObject = {
    if (cObj == null) {
      return null
    }
    val vmArr = getJsonArrayProp(cObj, PROP_CONTENT_VOICE_MUSICS)
    if (isEmpty(vmArr)) {
      return null
    }
    if (index >= vmArr.size()) {
      return null
    }
    vmArr.get(index).getAsJsonObject
  }

  private def reducePlayInfo(playInfos: mutable.Buffer[PlayInfoLog]): Array[(String, (Int, Boolean))] = {
    playInfos.map {
      p => {
        p.getResid -> (p.getOffset, PLAY_INFO_AUTOSWITCH.equalsIgnoreCase(p.getSwitchtype))
      }
    }.groupBy(_._1).mapValues(l => {
      var offset = Int.MaxValue
      var isFin = false
      l.foreach(ll => {
        offset = offset min ll._2._1
        isFin = isFin || ll._2._2
      })
      (offset, isFin)
    }).toArray
  }

  def reducePlayInfoByOffset(playInfos: mutable.Buffer[PlayInfoLog]): Array[(Int, List[PlayInfoLog])] = {
    if (playInfos == null || playInfos.size < 1) {
      return null
    }
    playInfos.map {
      p => {
        p.getOffset -> p
      }
    }.groupBy(_._1).mapValues(l => l.map(_._2).sortWith(_.startime < _.startime).toList).toArray
  }

  private def reducePlayInfoWithManSwitch(playInfos: mutable.Buffer[PlayInfoLog]): Array[(String, (Int, Boolean))] = {
    playInfos.filter(p => REFER_SEARCH.equalsIgnoreCase(p.getRefer))
      .map {
        p => {
          p.getResid -> (p.getOffset, PLAY_INFO_MAN_SWITCH.equalsIgnoreCase(p.getSwitchtype))
        }
      }.groupBy(_._1).mapValues(l => {
      var offset = Int.MaxValue
      var isMS = false
      l.foreach(ll => {
        offset = offset min ll._2._1
        isMS = isMS || ll._2._2
      })
      (offset, isMS)
    }).toArray
  }

  private def getEidFromJsonObject(jsonObject: JsonObject): String = {
    if (jsonObject == null) {
      return null
    }
    if (jsonObject.has(PROP_MUSIC_EID) && jsonObject.get(PROP_MUSIC_EID).isJsonPrimitive) {
      return jsonObject.get(PROP_MUSIC_EID).getAsString
    }
    null
  }

  private def getSlotOrDefault(slot: String): String = {
    if (StringUtils.isNotBlank(slot)) {
      return slot
    }
    INTENTION_SLOT_EMPTY
  }

  private def getSlotType(slot: String): String = {
    if (StringUtils.isBlank(slot)) {
      return "EMPTY"
    } else {
      if (slot.contains(MULTI_MARK)) {
        return "MULTI"
      } else if (slot.contains(NEG_MARK)) {
        return "NEG"
      }
    }
    "NORM"
  }

}
