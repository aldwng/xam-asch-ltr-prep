package model

final case class AppStoreContentStatistics(
  appId: Option[Long],
  imei: Option[String],
  packageName: Option[String],
  searchKeyword: Option[String], // 表示这个app对应的query词
  searchShowType: Option[String], // 取值：1表示一页霸屏(广告样式)  2 表示搜索增强  3表示富媒体(广告样式)，0或者null表示正常app列表样式
  ads: Option[Int], // 0非广告，1广告
  ref: Option[String], // ref="search" : 该限制条件后过滤出的记录都是搜索相关的记录
  refs: Option[String], /*searchEntry-input-searchResult-app   用户手动输入query进入搜索结果页
                                    searchEntry-searchHistory-searchResult-app  用户点击搜索历史记录进入搜索结果页
                                    searchEntry-hotSuggestion-searchResult-app   用户点击搜索热词进入搜索结果页
                                    searchEntry-suggestion-searchResult-app   用户点击suggest进入搜索结果页
                                    searchEntry-relatedSearch-searchResult-app 用户点击相关搜索进入搜索结果页*/
  appPos: Option[Int], // 表示app在搜索结果页的排序 (从0开始)
  exposure: Option[Int], // 曝光次数 （依据的是前端曝光打点日志）
  browse: Option[Int], // 进入app详情页的次数
  download: Option[Int], // 下载次数 （没有区分是从搜索结果页下载还是从详情页下载）
  install: Option[Int], // 安装次数
  active: Option[Int], // 激活次数
  source: Option[String]
)

final case class AppStorePvStatistics(
  appId: Option[Long],
  imei: Option[String],
  packageName: Option[String],
  searchKeyword: Option[String], // 表示这个app对应的query词
  ref: Option[String], // ref="search" : 该限制条件后过滤出的记录都是搜索相关的记录
  refs: Option[String],
  source: Option[String]
)
