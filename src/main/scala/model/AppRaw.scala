package model

import com.xiaomi.misearch.appsearch.rank.model.App
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.spark.broadcast.Broadcast
import utils.PathUtils.nonBlank

import scala.collection.JavaConverters._

/**
  * @author Shenglan Wang
  */
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

object AppRaw {
  def getApp(line: String, categoryMap: Broadcast[scala.collection.Map[Int, String]]): Option[App] = {
    val rawOption = decode[AppRaw](line).right.toOption
    if (rawOption.isDefined) {
      val raw = rawOption.get

      val keywords = raw.keywords.isDefined match {
        case true => raw.keywords.get.split(",").toSeq.filter(k => nonBlank(k)).map(_.trim)
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

      val data = new App(
        raw.id,
        raw.raw_id,
        raw.package_name,
        raw.display_name.getOrElse(""),
        raw.brief.getOrElse(""),
        keywords.asJava,
        level1Category.getOrElse(""),
        level2Category.getOrElse(""),
        raw.app_active_rank.getOrElse(0f),
        raw.app_cdr.getOrElse(0f),
        raw.app_download_rank.getOrElse(0f),
        raw.app_rank.getOrElse(0f)
      )
      Some(data)
    } else {
      None
    }
  }
}
