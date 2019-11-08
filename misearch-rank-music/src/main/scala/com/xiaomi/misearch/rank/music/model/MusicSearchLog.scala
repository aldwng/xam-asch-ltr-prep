package com.xiaomi.misearch.rank.music.model

/**
  * @author Shenglan Wang
  */
final case class MusicSearchLog(
                                 song: Option[String],
                                 musicid: Option[String],
                                 starttime: Option[Long],
                                 endTime: Option[Long],
                                 displaysong: Option[String],
                                 cp: Option[String],
                                 markinfo: Option[String],
                                 switchType: Option[String]
                               )

