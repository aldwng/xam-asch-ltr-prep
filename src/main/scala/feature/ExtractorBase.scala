package features

import com.xiaomi.misearch.appsearch.rank.group.FeaGroup
import com.xiaomi.miui.ad.appstore.feature.{BaseFea, QueryExtItem, RankInstance}
import com.xiaomi.misearch.appsearch.rank.similarity.impl.{Jaccard, SorensenDice}
import utils.FeaUtils._

import scala.collection.JavaConverters._

/**
  * Created by yun on 17-11-28.
  */
object ExtractorBase {
  final private val cat = "#"

  def extractFeatures(instance: RankInstance): List[BaseFea] = {
    var features = List[BaseFea]()
    val appItem  = instance.getApp
    val data     = instance.getData
    val query    = data.getQuery
    val querySeg = data.isSetQuerySeg match {
      case true  => data.getQuerySeg.asScala
      case false => Seq.empty
    }
    val queryExts = instance.isSetQueryExts match {
      case true  => instance.getQueryExts.asScala
      case false => Seq.empty
    }

    val qyTags = instance.isSetQyAppTags match {
      case true => instance.getQyAppTags.asScala
      case _    => Seq.empty
    }

    val qyKeywords = instance.isSetQyKeywords match {
      case true => instance.getQyKeywords.asScala
      case _    => Seq.empty
    }

//    val qySearchKeywords = instance.isSetQySearchKeywords match {
//      case true => instance.getQySearchKeywords.asScala
//      case _    => Seq.empty
//    }
//
//    val qyRelatedTags = instance.isSetQyRelatedTags match {
//      case true => instance.getQyRelatedTags.asScala
//      case _    => Seq.empty
//    }

    val qyCate1 = instance.isSetQyAppLevel1CategoryName match {
      case true => instance.getQyAppLevel1CategoryName.asScala
      case _    => Seq.empty
    }

    val qyCate2 = instance.isSetQyAppLevel2CategoryName match {
      case true => instance.getQyAppLevel2CategoryName.asScala
      case _    => Seq.empty
    }

//    val qyDisplayNames = instance.isSetQyDisplayNames match {
//      case true => instance.getQyDisplayNames.asScala
//      case _ => Seq.empty
//    }

    if (queryExts.nonEmpty) {
      features :+= makeFea(FeaGroup.qy_exist_ext.name, 1)
      features :+= makeFea(FeaGroup.qy_ext_num.name, queryExts.size)
    }

    if (qyTags.nonEmpty) {
      features :+= makeFea(FeaGroup.qy_exist_tags.name, 1)
      features :+= makeFea(FeaGroup.qy_tags_num.name, qyTags.size)
    }

    if (qyKeywords.nonEmpty) {
      features :+= makeFea(FeaGroup.qy_exist_keywords.name, 1)
      features :+= makeFea(FeaGroup.qy_keywords_num.name, qyKeywords.size)
    }

//    if (qyRelatedTags.nonEmpty) {
//      features :+= makeFea(FeaGroup.qy_exist_related_tags.name, 1)
//      features :+= makeFea(FeaGroup.qy_related_tags_num.name, qyRelatedTags.size)
//    }
//
//
//    if (qyDisplayNames.nonEmpty) {
//      features :+= makeFea(FeaGroup.qy_exist_displayname.name, 1)
//      features :+= makeFea(FeaGroup.qy_displayname_num.name, qyDisplayNames.size)
//    }

    if (instance.isSetQyAppIds) {
      features :+= makeFea(FeaGroup.qy_appid_num.name, instance.getQyAppIdsSize)
      features :+= makeFea(FeaGroup.qy_exist_appid.name, 1)
      if (instance.getQyAppIds.contains(appItem.getAppId)) {
        features :+= makeFea(FeaGroup.qy_appid_app_id_match.name, 1)
      }
    }

    features :+= makeFea(FeaGroup.qy_len.name, query.length)

    val (numCount, numRatio) = calcNumberCount(query)
    features :+= makeFea(FeaGroup.qy_numeric_num.name, numCount)
    features :+= makeFea(FeaGroup.qy_numeric_ratio.name, numRatio)

    val (charCount, charRatio) = calcCharCount(query)
    features :+= makeFea(FeaGroup.qy_char_num.name, charCount)
    features :+= makeFea(FeaGroup.qy_char_ratio.name, charRatio)

    val (chineseCount, chineseRatio) = calcChineseCount(query)
    features :+= makeFea(FeaGroup.qy_chinese_num.name, chineseCount)
    features :+= makeFea(FeaGroup.qy_chinese_ratio.name, chineseRatio)

    if (appItem.isSetCoclickTfIdf && instance.isSetQueryExtTfIdf) {
      val appKws = appItem.getCoclickTfIdf.asScala
      val qyKws  = instance.getQueryExtTfIdf.asScala
      features :+= makeFea(FeaGroup.qy_app_coclick_tf_idf.name + cat + "jac", jaccardScore(appKws, qyKws))
      features :+= makeFea(FeaGroup.qy_app_coclick_tf_idf.name + cat + "dice", diceScore(appKws, qyKws))
    }

    if (appItem.isSetDisplayName && appItem.getDisplayName.nonEmpty) {
      val dn = appItem.getDisplayName
      features :+= makeFea(FeaGroup.app_dn_len.name, dn.size)

      if (dn.equals(query)) {
        features :+= makeFea(FeaGroup.qy_app_dn_match.name, 1.0)
      }

      if (dn.contains(query)) {
        features :+= makeFea(FeaGroup.app_dn_contains_query.name, 1.0)
      }

      if (query.contains(dn)) {
        features :+= makeFea(FeaGroup.qy_contains_app_dn.name, 1.0)
      }

      val seg = appItem.isSetDisplayNameSeg match {
        case true  => appItem.getDisplayNameSeg.asScala
        case false => Seq.empty
      }

      features :+= makeFea(FeaGroup.qy_app_dn.name + cat + "jac" + cat + "bi", jaccardScore(2, query, dn))
      features :+= makeFea(FeaGroup.qy_app_dn.name + cat + "jac" + cat + "tri", jaccardScore(3, query, dn))
      features :+= makeFea(FeaGroup.qy_app_dn.name + cat + "jac" + cat + "seg", jaccardScore(querySeg, seg))

      features :+= makeFea(FeaGroup.qy_app_dn.name + cat + "dice" + cat + "bi", diceScore(2, query, dn))
      features :+= makeFea(FeaGroup.qy_app_dn.name + cat + "dice" + cat + "tri", diceScore(3, query, dn))
      features :+= makeFea(FeaGroup.qy_app_dn.name + cat + "dice" + cat + "seg", diceScore(querySeg, seg))

      features :+= makeFea(FeaGroup.qy_tags_app_dn_bi_sum.name + cat + "jac", calcTagJaccardSum(2, dn, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_dn_tri_sum.name + cat + "jac", calcTagJaccardSum(3, dn, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_dn_bi_sum.name + cat + "dice", calcTagDiceSum(2, dn, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_dn_tri_sum.name + cat + "dice", calcTagDiceSum(3, dn, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_dn_same_num.name, calcTagOverLap(dn, qyTags))

      features :+= makeFea(FeaGroup.qy_keywords_app_dn_bi_sum.name + cat + "jac", calcTagJaccardSum(2, dn, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_dn_tri_sum.name + cat + "jac", calcTagJaccardSum(3, dn, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_dn_bi_sum.name + cat + "dice", calcTagDiceSum(2, dn, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_dn_tri_sum.name + cat + "dice", calcTagDiceSum(3, dn, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_dn_same_num.name, calcTagOverLap(dn, qyKeywords))

//      features :+= makeFea(FeaGroup.qy_related_tags_app_dn_bi_sum.name + cat + "jac", calcTagJaccardSum(2,dn, qyRelatedTags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_dn_tri_sum.name + cat + "jac", calcTagJaccardSum(3,dn, qyRelatedTags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_dn_bi_sum.name + cat + "dice", calcTagDiceSum(2, dn, qyRelatedTags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_dn_tri_sum.name + cat + "dice", calcTagDiceSum(3, dn, qyRelatedTags))
//      if(qyRelatedTags.contains(dn)){
//        features :+= makeFea(FeaGroup.qy_related_tags_app_dn_same_num.name, 1)
//      }
//
//      features :+= makeFea(FeaGroup.qy_dn_app_dn_bi_sum.name + cat + "jac", calcTagJaccardSum(2,dn, qyDisplayNames))
//      features :+= makeFea(FeaGroup.qy_dn_app_dn_tri_sum.name + cat + "jac", calcTagJaccardSum(3,dn, qyDisplayNames))
//      features :+= makeFea(FeaGroup.qy_dn_app_dn_bi_sum.name + cat + "dice", calcTagDiceSum(2, dn, qyDisplayNames))
//      features :+= makeFea(FeaGroup.qy_dn_app_dn_tri_sum.name + cat + "dice", calcTagDiceSum(3, dn, qyDisplayNames))
//
//      if(qyDisplayNames.contains(dn)){
//        features :+= makeFea(FeaGroup.qy_dn_app_dn_same_num.name, 1)
//      }

      if (queryExts.nonEmpty) {
        features :+= makeFea(FeaGroup.qy_ext_app_dn.name + cat + "jac" + cat + "bi", calcQueryExtTextJaccardSum(2, queryExts, dn))
        features :+= makeFea(FeaGroup.qy_ext_app_dn.name + cat + "jac" + cat + "tri", calcQueryExtTextJaccardSum(3, queryExts, dn))
        features :+= makeFea(FeaGroup.qy_ext_app_dn.name + cat + "jac" + cat + "seg", calcQueryExtSegJaccardSum(queryExts, seg))

        features :+= makeFea(FeaGroup.qy_ext_app_dn.name + cat + "dice" + cat + "bi", calcQueryExtTextDiceSum(2, queryExts, dn))
        features :+= makeFea(FeaGroup.qy_ext_app_dn.name + cat + "dice" + cat + "tri", calcQueryExtTextDiceSum(3, queryExts, dn))
        features :+= makeFea(FeaGroup.qy_ext_app_dn.name + cat + "dice" + cat + "seg", calcQueryExtSegDiceSum(queryExts, seg))
      }
    }

    if (appItem.isSetDesc) {
      val desc = appItem.getDesc
      features :+= makeFea(FeaGroup.app_exist_desc.name, 1)

      val seg = appItem.isSetDescSeg match {
        case true  => appItem.getDescSeg.asScala
        case false => Seq.empty
      }

      features :+= makeFea(FeaGroup.qy_app_desc.name + cat + "jac" + cat + "seg", jaccardScore(querySeg, seg))
      features :+= makeFea(FeaGroup.qy_app_desc.name + cat + "dice" + cat + "seg", diceScore(querySeg, seg))

      features :+= makeFea(FeaGroup.qy_tags_app_desc_same_num.name, calcTagOverLap(desc, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_desc_seg.name + cat + "jac", jaccardScore(seg, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_desc_seg.name + cat + "dice", diceScore(seg, qyTags))

      features :+= makeFea(FeaGroup.qy_keywords_app_desc_same_num.name, calcTagOverLap(desc, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_desc_seg.name + cat + "jac", jaccardScore(seg, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_desc_seg.name + cat + "dice", diceScore(seg, qyKeywords))

//      features :+= makeFea(FeaGroup.qy_related_tags_app_desc_same_num.name, calcTagOverLap(desc, qyRelatedTags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_desc_seg.name + cat + "jac", jaccardScore(seg, qyRelatedTags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_desc_seg.name + cat + "dice", diceScore(seg, qyRelatedTags))

      features :+= makeFea(FeaGroup.qy_cate1_app_desc_same_num.name, calcTagOverLap(desc, qyCate1))
      features :+= makeFea(FeaGroup.qy_cate1_app_desc_seg.name + cat + "jac", jaccardScore(seg, qyCate1))
      features :+= makeFea(FeaGroup.qy_cate1_app_desc_seg.name + cat + "dice", diceScore(seg, qyCate1))

      features :+= makeFea(FeaGroup.qy_cate2_app_desc_same_num.name, calcTagOverLap(desc, qyCate2))
      features :+= makeFea(FeaGroup.qy_cate2_app_desc_seg.name + cat + "jac", jaccardScore(seg, qyCate2))
      features :+= makeFea(FeaGroup.qy_cate2_app_desc_seg.name + cat + "dice", diceScore(seg, qyCate2))

//      features :+= makeFea(FeaGroup.qy_dn_app_desc_same_num.name , calcTagOverLap(desc, qyDisplayNames))
//      features :+= makeFea(FeaGroup.qy_dn_app_desc_seg.name + cat + "jac", jaccardScore(seg, qyDisplayNames))
//      features :+= makeFea(FeaGroup.qy_dn_app_desc_seg.name + cat + "dice", diceScore(seg, qyDisplayNames))
      if (queryExts.nonEmpty) {
        features :+= makeFea(FeaGroup.qy_ext_app_desc.name + cat + "jac" + cat + "seg", calcQueryExtSegJaccardSum(queryExts, seg))
        features :+= makeFea(FeaGroup.qy_ext_app_desc.name + cat + "dice" + cat + "seg", calcQueryExtSegDiceSum(queryExts, seg))
      }

    }

    if (appItem.isSetBrief) {
      val brief = appItem.getBrief
      features :+= makeFea(FeaGroup.app_exist_brief.name, 1)

      val seg = appItem.isSetBriefSeg match {
        case true  => appItem.getBriefSeg.asScala
        case false => Seq.empty
      }
      features :+= makeFea(FeaGroup.qy_app_brief.name + cat + "jac" + cat + "bi", jaccardScore(2, query, brief))
      features :+= makeFea(FeaGroup.qy_app_brief.name + cat + "jac" + cat + "tri", jaccardScore(3, query, brief))
      features :+= makeFea(FeaGroup.qy_app_brief.name + cat + "jac" + cat + "seg", jaccardScore(querySeg, seg))

      features :+= makeFea(FeaGroup.qy_app_brief.name + cat + "dice" + cat + "bi", diceScore(2, query, brief))
      features :+= makeFea(FeaGroup.qy_app_brief.name + cat + "dice" + cat + "tri", diceScore(3, query, brief))
      features :+= makeFea(FeaGroup.qy_app_brief.name + cat + "dice" + cat + "seg", diceScore(querySeg, seg))

      features :+= makeFea(FeaGroup.qy_tags_app_brief_same_num.name, calcTagOverLap(brief, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_brief_seg.name + cat + "jac", jaccardScore(seg, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_brief_seg.name + cat + "dice", diceScore(seg, qyTags))

      features :+= makeFea(FeaGroup.qy_keywords_app_brief_same_num.name, calcTagOverLap(brief, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_brief_seg.name + cat + "jac", jaccardScore(seg, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_brief_seg.name + cat + "dice", diceScore(seg, qyKeywords))

//      features :+= makeFea(FeaGroup.qy_related_tags_app_brief_same_num.name, calcTagOverLap(brief, qyRelatedTags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_brief_seg.name + cat + "jac", jaccardScore(seg, qyRelatedTags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_brief_seg.name + cat + "dice", diceScore(seg, qyRelatedTags))

      features :+= makeFea(FeaGroup.qy_cate1_app_brief_same_num.name, calcTagOverLap(brief, qyCate1))
      features :+= makeFea(FeaGroup.qy_cate1_app_brief_seg.name + cat + "jac", jaccardScore(seg, qyCate1))
      features :+= makeFea(FeaGroup.qy_cate1_app_brief_seg.name + cat + "dice", diceScore(seg, qyCate1))

      features :+= makeFea(FeaGroup.qy_cate2_app_brief_same_num.name, calcTagOverLap(brief, qyCate2))
      features :+= makeFea(FeaGroup.qy_cate2_app_brief_seg.name + cat + "jac", jaccardScore(seg, qyCate2))
      features :+= makeFea(FeaGroup.qy_cate2_app_brief_seg.name + cat + "dice", diceScore(seg, qyCate2))

//      features :+= makeFea(FeaGroup.qy_dn_app_brief_same_num.name, calcTagOverLap(brief, qyDisplayNames))
//      features :+= makeFea(FeaGroup.qy_dn_app_brief_seg.name + cat + "jac", jaccardScore(seg, qyDisplayNames))
//      features :+= makeFea(FeaGroup.qy_dn_app_brief_seg.name + cat + "dice", diceScore(seg, qyDisplayNames))
      if (queryExts.nonEmpty) {
        features :+= makeFea(FeaGroup.qy_ext_app_brief.name + cat + "jac" + cat + "bi", calcQueryExtTextJaccardSum(2, queryExts, brief))
        features :+= makeFea(FeaGroup.qy_ext_app_brief.name + cat + "jac" + cat + "tri", calcQueryExtTextJaccardSum(3, queryExts, brief))
        features :+= makeFea(FeaGroup.qy_ext_app_brief.name + cat + "jac" + cat + "seg", calcQueryExtSegJaccardSum(queryExts, seg))

        features :+= makeFea(FeaGroup.qy_ext_app_brief.name + cat + "dice" + cat + "bi", calcQueryExtTextDiceSum(2, queryExts, brief))
        features :+= makeFea(FeaGroup.qy_ext_app_brief.name + cat + "dice" + cat + "tri", calcQueryExtTextDiceSum(3, queryExts, brief))
        features :+= makeFea(FeaGroup.qy_ext_app_brief.name + cat + "dice" + cat + "seg", calcQueryExtSegDiceSum(queryExts, seg))
      }
    }

    if (appItem.isSetLevel1CategoryName) {
      val cate1 = appItem.getLevel1CategoryName

      if (query.contains(cate1)) {
        features :+= makeFea(FeaGroup.qy_app_cate1.name + cat + "num", 1)
      }

      if (qyCate1.contains(cate1)) {
        features :+= makeFea(FeaGroup.qy_cate1_app_cate1_match.name, 1)
      }
      features :+= makeFea(FeaGroup.qy_tags_app_cate1_match.name, calcCateOverLap(cate1, qyTags))
      features :+= makeFea(FeaGroup.qy_keywords_app_cate1_match.name, calcCateOverLap(cate1, qyKeywords))
      // features :+= makeFea(FeaGroup.qy_related_tags_app_cate1_match.name, calcCateOverLap(cate1, qyRelatedTags))

      if (queryExts.nonEmpty) {
        val num = queryExts.map { ext =>
          val qyExt = ext.getQuery
          if (qyExt.contains(cate1)) {
            1
          } else {
            0
          }
        }.sum
        features :+= makeFea(FeaGroup.qy_ext_app_cate1.name + cat + "num", num)
      }
    }

    if (appItem.isSetLevel2CategoryName) {
      val cate2 = appItem.getLevel1CategoryName

      if (query.contains(cate2)) {
        features :+= makeFea(FeaGroup.qy_app_cate2.name + cat + "num", 1)
      }

      if (qyCate2.contains(cate2)) {
        features :+= makeFea(FeaGroup.qy_cate2_app_cate2_match.name, 1)
      }
      features :+= makeFea(FeaGroup.qy_tags_app_cate2_match.name, calcCateOverLap(cate2, qyTags))
      features :+= makeFea(FeaGroup.qy_keywords_app_cate2_match.name, calcCateOverLap(cate2, qyKeywords))
      // features :+= makeFea(FeaGroup.qy_related_tags_app_cate2_match.name, calcCateOverLap(cate2, qyRelatedTags))
      if (queryExts.nonEmpty) {
        val num = queryExts.map { ext =>
          val qyExt = ext.getQuery
          if (qyExt.contains(cate2)) {
            1
          } else {
            0
          }
        }.sum

        features :+= makeFea(FeaGroup.qy_ext_app_cate2.name + cat + "num", num)
      }
    }

    if (appItem.isSetTags) {
      val tags = appItem.getTags.asScala

      features :+= makeFea(FeaGroup.app_exist_tags.name, 1)
      features :+= makeFea(FeaGroup.app_tags_num.name, tags.size)

      features :+= makeFea(FeaGroup.qy_seg_app_tags.name + cat + "jac", jaccardScore(querySeg, tags))
      features :+= makeFea(FeaGroup.qy_seg_app_tags.name + cat + "dice", diceScore(querySeg, tags))

      features :+= makeFea(FeaGroup.qy_app_tags_same_num.name, calcTagOverLap(query, tags))
      features :+= makeFea(FeaGroup.qy_app_tags_bi_sum.name + cat + "jac", calcTagJaccardSum(2, query, tags))
      features :+= makeFea(FeaGroup.qy_app_tags_tri_sum.name + cat + "jac", calcTagJaccardSum(3, query, tags))
      features :+= makeFea(FeaGroup.qy_app_tags_bi_sum.name + cat + "dice", calcTagDiceSum(2, query, tags))
      features :+= makeFea(FeaGroup.qy_app_tags_tri_sum.name + cat + "dice", calcTagDiceSum(3, query, tags))

      features :+= makeFea(FeaGroup.qy_tags_app_tags_match.name + cat + "jac", jaccardScore(qyTags, tags))
      features :+= makeFea(FeaGroup.qy_tags_app_tags_match.name + cat + "dice", diceScore(qyTags, tags))
      features :+= makeFea(FeaGroup.qy_tags_app_tags_match.name + cat + "num", qyTags.intersect(tags).size)

      features :+= makeFea(FeaGroup.qy_keywords_app_tags_match.name + cat + "jac", jaccardScore(qyKeywords, tags))
      features :+= makeFea(FeaGroup.qy_keywords_app_tags_match.name + cat + "dice", diceScore(qyKeywords, tags))
      features :+= makeFea(FeaGroup.qy_keywords_app_tags_match.name + cat + "num", qyKeywords.intersect(tags).size)

      features :+= makeFea(FeaGroup.qy_cate1_app_tags_match.name + cat + "jac", jaccardScore(qyCate1, tags))
      features :+= makeFea(FeaGroup.qy_cate1_app_tags_match.name + cat + "dice", diceScore(qyCate1, tags))
      features :+= makeFea(FeaGroup.qy_cate1_app_tags_match.name + cat + "num", qyCate1.intersect(tags).size)

      features :+= makeFea(FeaGroup.qy_cate2_app_tags_match.name + cat + "jac", jaccardScore(qyCate2, tags))
      features :+= makeFea(FeaGroup.qy_cate2_app_tags_match.name + cat + "dice", diceScore(qyCate2, tags))
      features :+= makeFea(FeaGroup.qy_cate2_app_tags_match.name + cat + "num", qyCate2.intersect(tags).size)

//      features :+= makeFea(FeaGroup.qy_related_tags_app_tags_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, qyRelatedTags, tags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_tags_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, qyRelatedTags, tags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_tags_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2,qyRelatedTags, tags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_tags_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3,qyRelatedTags, tags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_tags_match.name + cat + "num", calcDisplayNamesTagsOverLap(qyRelatedTags, tags))
//
//      features :+= makeFea(FeaGroup.qy_dn_app_tags_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, qyDisplayNames, tags))
//      features :+= makeFea(FeaGroup.qy_dn_app_tags_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, qyDisplayNames, tags))
//      features :+= makeFea(FeaGroup.qy_dn_app_tags_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2,qyDisplayNames, tags))
//      features :+= makeFea(FeaGroup.qy_dn_app_tags_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3,qyDisplayNames, tags))
//      features :+= makeFea(FeaGroup.qy_dn_app_tags_match.name + cat + "num", calcDisplayNamesTagsOverLap(qyDisplayNames, tags))
      if (queryExts.nonEmpty) {
        features :+= makeFea(FeaGroup.qy_ext_app_tags.name + cat + "jac", calcQueryExtSegJaccardSum(queryExts, tags))
        features :+= makeFea(FeaGroup.qy_ext_app_tags.name + cat + "dice", calcQueryExtSegDiceSum(queryExts, tags))
        features :+= makeFea(FeaGroup.qy_ext_app_tags.name + cat + "num", calcQueryExtTagOverLap(queryExts, tags))

        features :+= makeFea(FeaGroup.qy_ext_app_tags_bi_sum.name + cat + "jac", calcQueryExtTagJaccardSum(2, queryExts, tags))
        features :+= makeFea(FeaGroup.qy_ext_app_tags_tri_sum.name + cat + "jac", calcQueryExtTagJaccardSum(3, queryExts, tags))
        features :+= makeFea(FeaGroup.qy_ext_app_tags_bi_sum.name + cat + "dice", calcQueryExtTagDiceSum(2, queryExts, tags))
        features :+= makeFea(FeaGroup.qy_ext_app_tags_tri_sum.name + cat + "dice", calcQueryExtTagDiceSum(3, queryExts, tags))
      }
    }

    if (appItem.isSetKeywords) {
      val keywords = appItem.getKeywords.asScala

      features :+= makeFea(FeaGroup.app_exist_keywords.name, 1)
      features :+= makeFea(FeaGroup.app_keywords_num.name, keywords.size)

      features :+= makeFea(FeaGroup.qy_seg_app_keywords.name + cat + "jac", jaccardScore(querySeg, keywords))
      features :+= makeFea(FeaGroup.qy_seg_app_keywords.name + cat + "dice", diceScore(querySeg, keywords))

      features :+= makeFea(FeaGroup.qy_app_keywords_same_num.name, calcTagOverLap(query, keywords))
      features :+= makeFea(FeaGroup.qy_app_keywords_bi_sum.name + cat + "jac", calcTagJaccardSum(2, query, keywords))
      features :+= makeFea(FeaGroup.qy_app_keywords_tri_sum.name + cat + "jac", calcTagJaccardSum(3, query, keywords))
      features :+= makeFea(FeaGroup.qy_app_keywords_bi_sum.name + cat + "dice", calcTagDiceSum(2, query, keywords))
      features :+= makeFea(FeaGroup.qy_app_keywords_tri_sum.name + cat + "dice", calcTagDiceSum(3, query, keywords))

      features :+= makeFea(FeaGroup.qy_keywords_app_keywords_match.name + cat + "jac", jaccardScore(qyKeywords, keywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_keywords_match.name + cat + "dice", diceScore(qyKeywords, keywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_keywords_match.name + cat + "num", qyKeywords.intersect(keywords).size)

      features :+= makeFea(FeaGroup.qy_tags_app_keywords_match.name + cat + "jac", jaccardScore(qyTags, keywords))
      features :+= makeFea(FeaGroup.qy_tags_app_keywords_match.name + cat + "dice", diceScore(qyTags, keywords))
      features :+= makeFea(FeaGroup.qy_tags_app_keywords_match.name + cat + "num", qyTags.intersect(keywords).size)

//      features :+= makeFea(FeaGroup.qy_related_tags_app_keywords_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, qyRelatedTags, keywords))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_keywords_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, qyRelatedTags, keywords))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_keywords_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2,qyRelatedTags, keywords))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_keywords_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3,qyRelatedTags, keywords))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_keywords_match.name + cat + "num", calcDisplayNamesTagsOverLap(qyRelatedTags, keywords))

      features :+= makeFea(FeaGroup.qy_cate1_app_keywords_match.name + cat + "jac", jaccardScore(qyCate1, keywords))
      features :+= makeFea(FeaGroup.qy_cate1_app_keywords_match.name + cat + "dice", diceScore(qyCate1, keywords))
      features :+= makeFea(FeaGroup.qy_cate1_app_keywords_match.name + cat + "num", qyCate1.intersect(keywords).size)

      features :+= makeFea(FeaGroup.qy_cate2_app_keywords_match.name + cat + "jac", jaccardScore(qyCate2, keywords))
      features :+= makeFea(FeaGroup.qy_cate2_app_keywords_match.name + cat + "dice", diceScore(qyCate2, keywords))
      features :+= makeFea(FeaGroup.qy_cate2_app_keywords_match.name + cat + "num", qyCate2.intersect(keywords).size)

//      features :+= makeFea(FeaGroup.qy_dn_app_keywords_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, qyDisplayNames, keywords))
//      features :+= makeFea(FeaGroup.qy_dn_app_keywords_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, qyDisplayNames, keywords))
//      features :+= makeFea(FeaGroup.qy_dn_app_keywords_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2,qyDisplayNames, keywords))
//      features :+= makeFea(FeaGroup.qy_dn_app_keywords_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3,qyDisplayNames, keywords))
//      features :+= makeFea(FeaGroup.qy_dn_app_keywords_match.name + cat + "num", calcDisplayNamesTagsOverLap(qyDisplayNames, keywords))
      if (queryExts.nonEmpty) {
        features :+= makeFea(FeaGroup.qy_ext_app_keywords.name + cat + "num", calcQueryExtTagOverLap(queryExts, keywords))
        features :+= makeFea(FeaGroup.qy_ext_app_keywords.name + cat + "jac", calcQueryExtSegJaccardSum(queryExts, keywords))
        features :+= makeFea(FeaGroup.qy_ext_app_keywords.name + cat + "dice", calcQueryExtSegDiceSum(queryExts, keywords))
        features :+= makeFea(FeaGroup.qy_ext_app_keywords_bi_sum.name + cat + "jac", calcQueryExtTagJaccardSum(2, queryExts, keywords))
        features :+= makeFea(FeaGroup.qy_ext_app_keywords_tri_sum.name + cat + "jac", calcQueryExtTagJaccardSum(3, queryExts, keywords))
        features :+= makeFea(FeaGroup.qy_ext_app_keywords_bi_sum.name + cat + "dice", calcQueryExtTagDiceSum(2, queryExts, keywords))
        features :+= makeFea(FeaGroup.qy_ext_app_keywords_tri_sum.name + cat + "dice", calcQueryExtTagDiceSum(3, queryExts, keywords))
      }
    }

    if (appItem.isSetSearchKeywords) {
      val searchKeywords = appItem.getSearchKeywords.asScala
      features :+= makeFea(FeaGroup.app_exist_search_keywords.name, 1)
      features :+= makeFea(FeaGroup.app_search_keywords_num.name, searchKeywords.size)
      features :+= makeFea(FeaGroup.qy_app_search_keywords_same_num.name, calcTagOverLap(query, searchKeywords))

      features :+= makeFea(FeaGroup.qy_seg_app_search_keywords.name + cat + "jac", jaccardScore(querySeg, searchKeywords))
      features :+= makeFea(FeaGroup.qy_seg_app_search_keywords.name + cat + "dice", diceScore(querySeg, searchKeywords))

      features :+= makeFea(FeaGroup.qy_app_search_keywords_bi_sum.name + cat + "jac", calcTagJaccardSum(2, query, searchKeywords))
      features :+= makeFea(FeaGroup.qy_app_search_keywords_tri_sum.name + cat + "jac", calcTagJaccardSum(3, query, searchKeywords))
      features :+= makeFea(FeaGroup.qy_app_search_keywords_bi_sum.name + cat + "dice", calcTagDiceSum(2, query, searchKeywords))
      features :+= makeFea(FeaGroup.qy_app_search_keywords_tri_sum.name + cat + "dice", calcTagDiceSum(3, query, searchKeywords))

//      features :+= makeFea(FeaGroup.qy_search_keywords_app_search_keywords_match.name + cat + "jac", jaccardScore(qySearchKeywords, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_search_keywords_app_search_keywords_match.name + cat + "dice", diceScore(qySearchKeywords, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_search_keywords_app_search_keywords_match.name + cat + "num", qySearchKeywords.intersect(searchKeywords).size)

      features :+= makeFea(FeaGroup.qy_tags_app_search_keywords_match.name + cat + "jac", jaccardScore(qyTags, searchKeywords))
      features :+= makeFea(FeaGroup.qy_tags_app_search_keywords_match.name + cat + "dice", diceScore(qyTags, searchKeywords))
      features :+= makeFea(FeaGroup.qy_tags_app_search_keywords_match.name + cat + "num", qyTags.intersect(searchKeywords).size)

      features :+= makeFea(FeaGroup.qy_keywords_app_search_keywords_match.name + cat + "jac", jaccardScore(qyKeywords, searchKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_search_keywords_match.name + cat + "dice", diceScore(qyKeywords, searchKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_search_keywords_match.name + cat + "num", qyKeywords.intersect(searchKeywords).size)

//      features :+= makeFea(FeaGroup.qy_related_tags_app_search_keywords_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, qyRelatedTags, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_search_keywords_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, qyRelatedTags, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_search_keywords_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2,qyRelatedTags, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_search_keywords_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3,qyRelatedTags, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_search_keywords_match.name + cat + "num", calcDisplayNamesTagsOverLap(qyRelatedTags, searchKeywords))

      features :+= makeFea(FeaGroup.qy_cate1_app_search_keywords_match.name + cat + "jac", jaccardScore(qyCate1, searchKeywords))
      features :+= makeFea(FeaGroup.qy_cate1_app_search_keywords_match.name + cat + "dice", diceScore(qyCate1, searchKeywords))
      features :+= makeFea(FeaGroup.qy_cate1_app_search_keywords_match.name + cat + "num", qyCate1.intersect(searchKeywords).size)

      features :+= makeFea(FeaGroup.qy_cate2_app_search_keywords_match.name + cat + "jac", jaccardScore(qyCate2, searchKeywords))
      features :+= makeFea(FeaGroup.qy_cate2_app_search_keywords_match.name + cat + "dice", diceScore(qyCate2, searchKeywords))
      features :+= makeFea(FeaGroup.qy_cate2_app_search_keywords_match.name + cat + "num", qyCate2.intersect(searchKeywords).size)

//      features :+= makeFea(FeaGroup.qy_dn_app_search_keywords_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, qyDisplayNames, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_dn_app_search_keywords_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, qyDisplayNames, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_dn_app_search_keywords_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2,qyDisplayNames, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_dn_app_search_keywords_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3,qyDisplayNames, searchKeywords))
//      features :+= makeFea(FeaGroup.qy_dn_app_search_keywords_match.name + cat + "num", calcDisplayNamesTagsOverLap(qyDisplayNames, searchKeywords))
      if (queryExts.nonEmpty) {
        features :+= makeFea(FeaGroup.qy_ext_app_search_keywords.name + cat + "num", calcQueryExtTagOverLap(queryExts, searchKeywords))
        features :+= makeFea(FeaGroup.qy_ext_app_search_keywords.name + cat + "jac", calcQueryExtSegJaccardSum(queryExts, searchKeywords))
        features :+= makeFea(FeaGroup.qy_ext_app_search_keywords.name + cat + "dice", calcQueryExtSegDiceSum(queryExts, searchKeywords))
        features :+= makeFea(FeaGroup.qy_ext_app_search_keywords_bi_sum.name + cat + "jac", calcQueryExtTagJaccardSum(2, queryExts, searchKeywords))
        features :+= makeFea(FeaGroup.qy_ext_app_search_keywords_tri_sum.name + cat + "jac", calcQueryExtTagJaccardSum(3, queryExts, searchKeywords))
        features :+= makeFea(FeaGroup.qy_ext_app_search_keywords_bi_sum.name + cat + "dice", calcQueryExtTagDiceSum(2, queryExts, searchKeywords))
        features :+= makeFea(FeaGroup.qy_ext_app_search_keywords_tri_sum.name + cat + "dice", calcQueryExtTagDiceSum(3, queryExts, searchKeywords))
      }
    }

    if (appItem.isSetRelatedTags) {
      val relatedTags = appItem.getRelatedTags.asScala

      features :+= makeFea(FeaGroup.app_exist_related_tags.name, 1)
      features :+= makeFea(FeaGroup.app_related_tags_num.name, relatedTags.size)
      features :+= makeFea(FeaGroup.qy_app_related_tags_same_num.name, calcTagOverLap(query, relatedTags))

      features :+= makeFea(FeaGroup.qy_seg_app_related_tags.name + cat + "jac", jaccardScore(querySeg, relatedTags))
      features :+= makeFea(FeaGroup.qy_seg_app_related_tags.name + cat + "dice", diceScore(querySeg, relatedTags))

      features :+= makeFea(FeaGroup.qy_app_related_tags_bi_sum.name + cat + "jac", calcTagJaccardSum(2, query, relatedTags))
      features :+= makeFea(FeaGroup.qy_app_related_tags_tri_sum.name + cat + "jac", calcTagJaccardSum(3, query, relatedTags))
      features :+= makeFea(FeaGroup.qy_app_related_tags_bi_sum.name + cat + "dice", calcTagDiceSum(2, query, relatedTags))
      features :+= makeFea(FeaGroup.qy_app_related_tags_tri_sum.name + cat + "dice", calcTagDiceSum(3, query, relatedTags))

//      features :+= makeFea(FeaGroup.qy_related_tags_app_related_tags_match.name + cat + "jac", jaccardScore(relatedTags, qyRelatedTags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_related_tags_match.name + cat + "dice", diceScore(relatedTags, qyRelatedTags))
//      features :+= makeFea(FeaGroup.qy_related_tags_app_related_tags_match.name + cat + "num", qyRelatedTags.intersect(relatedTags).size)

      features :+= makeFea(FeaGroup.qy_tags_app_related_tags_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, relatedTags, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_related_tags_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, relatedTags, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_related_tags_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2, relatedTags, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_related_tags_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3, relatedTags, qyTags))
      features :+= makeFea(FeaGroup.qy_tags_app_related_tags_match.name + cat + "num", calcDisplayNamesTagsOverLap(relatedTags, qyTags))

      features :+= makeFea(FeaGroup.qy_keywords_app_related_tags_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, relatedTags, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_related_tags_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, relatedTags, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_related_tags_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2, relatedTags, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_related_tags_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3, relatedTags, qyKeywords))
      features :+= makeFea(FeaGroup.qy_keywords_app_related_tags_match.name, calcDisplayNamesTagsOverLap(relatedTags, qyKeywords))

      features :+= makeFea(FeaGroup.qy_cate1_app_related_tags_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, relatedTags, qyCate1))
      features :+= makeFea(FeaGroup.qy_cate1_app_related_tags_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, relatedTags, qyCate1))
      features :+= makeFea(FeaGroup.qy_cate1_app_related_tags_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2, relatedTags, qyCate1))
      features :+= makeFea(FeaGroup.qy_cate1_app_related_tags_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3, relatedTags, qyCate1))
      features :+= makeFea(FeaGroup.qy_cate1_app_related_tags_match.name, calcDisplayNamesTagsOverLap(relatedTags, qyCate1))

      features :+= makeFea(FeaGroup.qy_cate2_app_related_tags_bi_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(2, relatedTags, qyCate2))
      features :+= makeFea(FeaGroup.qy_cate2_app_related_tags_tri_sum.name + cat + "jac", calcDisplayNamesTagsJaccardSum(3, relatedTags, qyCate2))
      features :+= makeFea(FeaGroup.qy_cate2_app_related_tags_bi_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(2, relatedTags, qyCate2))
      features :+= makeFea(FeaGroup.qy_cate2_app_related_tags_tri_sum.name + cat + "dice", calcDisplayNamesTagsDiceSum(3, relatedTags, qyCate2))
      features :+= makeFea(FeaGroup.qy_cate2_app_related_tags_match.name, calcDisplayNamesTagsOverLap(relatedTags, qyCate2))

//      features :+= makeFea(FeaGroup.qy_dn_app_related_tags_match.name + cat + "jac", jaccardScore(relatedTags, qyDisplayNames))
//      features :+= makeFea(FeaGroup.qy_dn_app_related_tags_match.name + cat + "dice", diceScore(relatedTags, qyDisplayNames))
//      features :+= makeFea(FeaGroup.qy_dn_app_related_tags_match.name + cat + "num", qyDisplayNames.intersect(relatedTags).size)
      if (queryExts.nonEmpty) {
        features :+= makeFea(FeaGroup.qy_ext_app_related_tags.name + cat + "num", calcQueryExtTagOverLap(queryExts, relatedTags))
        features :+= makeFea(FeaGroup.qy_ext_app_related_tags.name + cat + "jac", calcQueryExtSegJaccardSum(queryExts, relatedTags))
        features :+= makeFea(FeaGroup.qy_ext_app_related_tags.name + cat + "dice", calcQueryExtSegDiceSum(queryExts, relatedTags))
        features :+= makeFea(FeaGroup.qy_ext_app_related_tags_bi_sum.name + cat + "jac", calcQueryExtTagJaccardSum(2, queryExts, relatedTags))
        features :+= makeFea(FeaGroup.qy_ext_app_related_tags_tri_sum.name + cat + "jac", calcQueryExtTagJaccardSum(3, queryExts, relatedTags))
        features :+= makeFea(FeaGroup.qy_ext_app_related_tags_bi_sum.name + cat + "dice", calcQueryExtTagDiceSum(2, queryExts, relatedTags))
        features :+= makeFea(FeaGroup.qy_ext_app_related_tags_tri_sum.name + cat + "dice", calcQueryExtTagDiceSum(3, queryExts, relatedTags))
      }
    }

    if (appItem.isSetYstdBrowser) {
      features :+= makeFea(FeaGroup.app_ystd_browser.name, appItem.getYstdBrowser)
    }
    if (appItem.isSetYstdDownload) {
      features :+= makeFea(FeaGroup.app_ystd_download.name, appItem.getYstdDownload)
    }
    if (appItem.isSetYstdInstall) {
      features :+= makeFea(FeaGroup.app_ystd_install.name, appItem.getYstdInstall)
    }
    if (appItem.isSetYstdActive) {
      features :+= makeFea(FeaGroup.app_ystd_active.name, appItem.getYstdActive)
    }

    if (appItem.isSetLastWeekBrowser) {
      features :+= makeFea(FeaGroup.app_last_week_browser.name, appItem.getLastWeekBrowser)
    }
    if (appItem.isSetLastWeekDownload) {
      features :+= makeFea(FeaGroup.app_last_week_download.name, appItem.getLastWeekDownload)
    }
    if (appItem.isSetLastWeekInstall) {
      features :+= makeFea(FeaGroup.app_last_week_install.name, appItem.getLastWeekInstall)
    }
    if (appItem.isSetLastWeekActive) {
      features :+= makeFea(FeaGroup.app_last_week_active.name, appItem.getLastWeekActive)
    }

    if (appItem.isSetLastMonthBrowser) {
      features :+= makeFea(FeaGroup.app_last_month_browser.name, appItem.getLastMonthBrowser)
    }
    if (appItem.isSetLastMonthDownload) {
      features :+= makeFea(FeaGroup.app_last_month_download.name, appItem.getLastMonthDownload)
    }
    if (appItem.isSetLastMonthInstall) {
      features :+= makeFea(FeaGroup.app_last_month_install.name, appItem.getLastMonthInstall)
    }
    if (appItem.isSetLastMonthActive) {
      features :+= makeFea(FeaGroup.app_last_month_active.name, appItem.getLastMonthActive)
    }

    //    if (appItem.isSetApkSize) {
    //      features :+= makeFea(FeaGroup.app_apk_size.name, appItem.getApkSize)
    //    }
    //
    //    if (appItem.isSetStar) {
    //      features :+= makeFea(FeaGroup.app_star.name, appItem.getStar)
    //    }
    //
    //    if (appItem.isSetGoodCmt) {
    //      features :+= makeFea(FeaGroup.app_good_cmt.name, appItem.getGoodCmt)
    //    }
    //
    //    if (appItem.isSetBadCmt) {
    //      features :+= makeFea(FeaGroup.app_bad_cmt.name, appItem.getBadCmt)
    //    }

    if (appItem.isSetDeveloperAppCount) {
      features :+= makeFea(FeaGroup.app_developer_app_count.name, appItem.getDeveloperAppCount)
    }

//    if (appItem.isSetRankOrder) {
//      features :+= makeFea(FeaGroup.app_rank_order.name, appItem.getRankOrder)
//    }

    //    if(appItem.isSetRankOrderForPad){
    //      features :+= makeFea(FeaGroup.app_rank_order_for_pad.name, appItem.getRankOrderForPad)
    //    }

    if (appItem.isSetLastWeekUpdateCount) {
      features :+= makeFea(FeaGroup.app_last_week_update_count.name, appItem.getLastWeekUpdateCount)
    }

    if (appItem.isSetLastMonthUpdateCount) {
      features :+= makeFea(FeaGroup.app_last_month_update_count.name, appItem.getLastMonthUpdateCount)
    }

    if (appItem.isSetFavoriteCount) {
      features :+= makeFea(FeaGroup.app_favorite_count.name, appItem.getFavoriteCount)
    }

    if (appItem.isSetFeedbackCount) {
      features :+= makeFea(FeaGroup.app_feedback_count.name, appItem.getFeedbackCount)
    }
    //    if(appItem.isSetPermissionCount){
    //      features :+= makeFea(FeaGroup.app_permission_count.name, appItem.getPermissionCount)
    //    }
    //
    if (appItem.isSetAppActiveRank) {
      features :+= makeFea(FeaGroup.app_active_rank.name, appItem.getAppActiveRank)
    }

    if (appItem.isSetAppCdr) {
      features :+= makeFea(FeaGroup.app_cdr.name, appItem.getAppCdr)
    }

    if (appItem.isSetAppDownloadRank) {
      features :+= makeFea(FeaGroup.app_download_rank.name, appItem.getAppDownloadRank)
    }

    if (appItem.isSetAppHot) {
      features :+= makeFea(FeaGroup.app_hot.name, appItem.getAppHot)
    }

    if (appItem.isSetAppRank) {
      features :+= makeFea(FeaGroup.app_rank.name, appItem.getAppRank)
    }

    //    if (appItem.isSetGameArpu) {
    //      features :+= makeFea(FeaGroup.game_arpu.name, appItem.getGameArpu)
    //    }
    //
    //    if (appItem.isSetGameCdr) {
    //      features :+= makeFea(FeaGroup.game_cdr.name, appItem.getGameCdr)
    //    }
    //
    if (appItem.isSetRatingScore) {
      features :+= makeFea(FeaGroup.app_rating_score.name, appItem.getRatingScore)
    }

    if (appItem.isSetPublisher) {
      val publisher = appItem.getPublisher
      if (instance.isSetQyPublisher && instance.getQyPublisher.contains(publisher)) {
        features :+= makeFea(FeaGroup.qy_publisher_app_publisher_match.name, 1)
      }
      val seg = querySeg.filter(q => publisher.contains(q))
      if (seg.nonEmpty) {
        features :+= makeFea(FeaGroup.qy_seg_app_publisher.name, seg.size)
      }
    }

    features
  }

  def jaccardScore(num: Int, query: String, text: String): Double = {
    if (query.size > num || text.size >= num) {
      val jaccard = new Jaccard(num)
      jaccard.similarity(query, text)
    } else {
      0
    }
  }

  def jaccardScore(query: Seq[String], words: Seq[String]): Double = {
    if (query.size > 0 && words.size > 0) {
      val jaccard = new Jaccard()
      jaccard.similarity(query.distinct.asJava, words.distinct.asJava)
    } else {
      0
    }
  }

  def diceScore(num: Int, query: String, text: String): Double = {
    if (query.size > num || text.size >= num) {
      val dice = new SorensenDice(num)
      dice.similarity(query, text)
    } else {
      0
    }
  }

  def diceScore(query: Seq[String], words: Seq[String]): Double = {
    if (query.size > 0 && words.size > 0) {
      val dice = new SorensenDice()
      dice.similarity(query.distinct.asJava, words.distinct.asJava)
    } else {
      0
    }
  }

  def calcStrOverLap(query: String, text: String): (Double, Double) = {
    val same = query.intersect(text)
    (same.size, same.size.toDouble / query.size)
  }

  def calcTagOverLap(query: String, tags: Seq[String]): Double = {
    tags.map { tag =>
      query.contains(tag) match {
        case true  => 1.0
        case false => 0.0
      }
    }.sum
  }

  def calcCateOverLap(cate: String, tags: Seq[String]): Double = {
    tags.map { tag =>
      tag.contains(cate) match {
        case true => 1.0
        case _    => 0.0
      }
    }.sum
  }

  def calcCatesOverLap(cates: Seq[String], tags: Seq[String]): Double = {
    cates.map { cate =>
      calcCateOverLap(cate, tags)
    }.sum
  }

  def calcTagJaccardSum(num: Int, query: String, tags: Seq[String]): Double = {
    tags.map { t =>
      jaccardScore(num, query, t)
    }.sum
  }

  def calcTagDiceSum(num: Int, query: String, tags: Seq[String]): Double = {
    tags.map { t =>
      diceScore(num, query, t)
    }.sum
  }

  def calcDisplayNamesTagsJaccardSum(num: Int, displayNames: Seq[String], tags: Seq[String]): Double = {
    displayNames.map { dn =>
      calcTagJaccardSum(num, dn, tags)
    }.sum
  }

  def calcDisplayNamesTagsDiceSum(num: Int, displayNames: Seq[String], tags: Seq[String]): Double = {
    displayNames.map { dn =>
      calcTagDiceSum(num, dn, tags)
    }.sum
  }

  def calcDisplayNamesTagsOverLap(displayNames: Seq[String], tags: Seq[String]): Double = {
    displayNames.map { dn =>
      calcTagOverLap(dn, tags)
    }.sum
  }

  def calcNumberCount(query: String): (Double, Double) = {
    val num = query.map { ch =>
      ch >= '0' && ch <= '9' match {
        case true  => 1.0
        case false => 0.0
      }
    }.sum
    (num, num / query.size)
  }

  def calcCharCount(query: String): (Double, Double) = {
    val num = query.map { ch =>
      (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') match {
        case true  => 1.0
        case false => 0.0
      }
    }.sum
    (num, num / query.size)
  }

  def calcChineseCount(query: String): (Double, Double) = {
    val num = query.map { ch =>
      val len = Integer.toBinaryString(ch)
      if (len.size > 8) {
        1.0
      } else {
        0.0
      }
    }.sum
    (num, num / query.size)
  }

  def calcQueryExtTextJaccardSum(num: Int, queryExts: Seq[QueryExtItem], text: String): Double = {
    if (queryExts.isEmpty) {
      0.0
    } else {
      queryExts.map { ext =>
        val qyExt = ext.getQuery
        jaccardScore(num, qyExt, text)
      }.sum
    }
  }

  def calcQueryExtTextDiceSum(num: Int, queryExts: Seq[QueryExtItem], text: String): Double = {
    if (queryExts.isEmpty) {
      0.0
    } else {
      queryExts.map { ext =>
        val qyExt = ext.getQuery
        diceScore(num, qyExt, text)
      }.sum
    }
  }

  def calcQueryExtSegJaccardSum(queryExts: Seq[QueryExtItem], seg: Seq[String]): Double = {
    if (queryExts.isEmpty || seg.isEmpty) {
      0.0
    } else {
      queryExts.map { ext =>
        val extSeg = ext.isSetQuerySeg match {
          case true  => ext.getQuerySeg.asScala
          case false => Seq.empty
        }
        jaccardScore(extSeg, seg)
      }.sum
    }
  }

  def calcQueryExtSegDiceSum(queryExts: Seq[QueryExtItem], seg: Seq[String]): Double = {
    if (queryExts.isEmpty || seg.isEmpty) {
      0.0
    } else {
      queryExts.map { ext =>
        val extSeg = ext.isSetQuerySeg match {
          case true  => ext.getQuerySeg.asScala
          case false => Seq.empty
        }
        diceScore(extSeg, seg)
      }.sum
    }
  }

  def calcQueryExtTagOverLap(queryExts: Seq[QueryExtItem], tags: Seq[String]): Double = {
    if (queryExts.isEmpty || tags.isEmpty) {
      0.0
    } else {
      queryExts.map { ext =>
        val query = ext.getQuery
        calcTagOverLap(query, tags)
      }.sum
    }
  }

  def calcQueryExtTagJaccardSum(num: Int, queryExts: Seq[QueryExtItem], tags: Seq[String]): Double = {
    if (queryExts.isEmpty || tags.isEmpty) {
      0.0
    } else {
      queryExts.map { ext =>
        val query = ext.getQuery
        calcTagJaccardSum(num, query, tags)
      }.sum
    }
  }

  def calcQueryExtTagDiceSum(num: Int, queryExts: Seq[QueryExtItem], tags: Seq[String]): Double = {
    if (queryExts.isEmpty || tags.isEmpty) {
      0.0
    } else {
      queryExts.map { ext =>
        val query = ext.getQuery
        calcTagDiceSum(num, query, tags)
      }.sum
    }
  }
}
