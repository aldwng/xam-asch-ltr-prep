package model

import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.spark.broadcast.Broadcast
import utils.PathUtils.nonBlank

/**
  * @author Shenglan Wang
  */
final case class App(
                      appActiveRank: Option[Double],
                      appCdr: Option[Double],
                      appDownloadRank: Option[Double],
                      appHot: Option[Double],
                      appRank: Option[Double],
                      displayName: Option[String],
                      displayNameLen: Option[Int],
                      folderTags: Seq[String],
                      gameArpu: Option[Double],
                      gameCdr: Option[Double],
                      gameRank: Option[Double],
                      id: String,
                      introduction: Option[String],
                      brief: Option[String],
                      packageName: String,
                      ratingScore: Option[Double],
                      rawId: String,
                      keywords: Seq[String],
                      relatedTags: Seq[String],
                      searchKeywords: Seq[String],
                      tagNames: Seq[String],
                      publisher: Option[String],
                      wdjCategory: Option[String],
                      level1Category: Option[String],
                      level2Category: Option[String]
                    )

final case class AppRaw(
                         app_active_rank: Option[Double],
                         app_cdr: Option[Double],
                         app_download_rank: Option[Double],
                         app_hot: Option[Double],
                         app_rank: Option[Double],
                         display_name: Option[String],
                         display_name_len: Option[Int],
                         folder_tags: Option[String],
                         game_arpu: Option[Double],
                         game_cdr: Option[Double],
                         game_rank: Option[Double],
                         id: String,
                         introduction: Option[String],
                         brief: Option[String],
                         package_name: String,
                         ratingScore: Option[Double],
                         raw_id: String,
                         keywords: Option[String],
                         related_tags: Option[String],
                         search_keywords: Option[String],
                         tagNames: Option[Seq[String]],
                         publisher: Option[String],
                         wdj_category: Option[String],
                         level1_category_id: Option[Int],
                         level2_category_id: Option[Int]
                       )

object App {
  def getApp(line: String, categoryMap: Broadcast[scala.collection.Map[Int, String]]): Option[App] = {
    val rawOption = decode[AppRaw](line).right.toOption
    if (rawOption.isDefined) {
      val raw = rawOption.get

      val folder_tags = raw.folder_tags.isDefined match {
        case true => raw.folder_tags.get.split(",").toSeq.filter(k => nonBlank(k)).map(_.trim)
        case _ => Seq.empty
      }

      val keywords = raw.keywords.isDefined match {
        case true => raw.keywords.get.split(",").toSeq.filter(k => nonBlank(k)).map(_.trim)
        case _ => Seq.empty
      }

      val related_tags = raw.related_tags.isDefined match {
        case true => raw.related_tags.get.split(",").toSeq.filter(k => nonBlank(k)).map(_.trim)
        case _ => Seq.empty
      }
      val search_keywords = raw.search_keywords.isDefined match {
        case true => raw.search_keywords.get.split(";").toSeq.filter(k => nonBlank(k)).map(_.trim)
        case _ => Seq.empty
      }

      val tagNames = raw.tagNames.isDefined match {
        case true => raw.tagNames.get
        case _ => Seq.empty
      }

      val level1Category = raw.level1_category_id.isDefined match {
        case true => {
          categoryMap.value.contains(raw.level1_category_id.get) match {
            case true => Some(categoryMap.value(raw.level1_category_id.get))
            case _ => None
          }
        }
        case _ => None
      }

      val level2Category = raw.level2_category_id.isDefined match {
        case true => {
          categoryMap.value.contains(raw.level2_category_id.get) match {
            case true => Some(categoryMap.value(raw.level2_category_id.get))
            case _ => None
          }
        }
        case _ => None
      }

      val data = App(
        raw.app_active_rank,
        raw.app_cdr,
        raw.app_download_rank,
        raw.app_hot,
        raw.app_rank,
        raw.display_name,
        raw.display_name_len,
        folder_tags,
        raw.game_arpu,
        raw.game_cdr,
        raw.game_rank,
        raw.id,
        raw.introduction,
        raw.brief,
        raw.package_name,
        raw.ratingScore,
        raw.raw_id,
        keywords,
        related_tags,
        search_keywords,
        tagNames,
        raw.publisher,
        raw.wdj_category,
        level1Category,
        level2Category
      )
      Some(data)
    } else {
      None
    }
  }
}
