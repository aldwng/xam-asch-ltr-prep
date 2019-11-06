namespace java com.xiaomi.miui.ad.appstore.feature

struct DataRaw {
    1:optional string query;
    2:optional i64 appId;
    3:optional i32 exposure;
    4:optional i32 download;
    5:optional double dtr;
    6:optional i32 label;
    7:optional list<string> querySeg;
}

struct Cate{
    1:optional i32 id;
    2:optional string name;
    3:optional double score;
}

struct SimilarApp{
    1:optional string packageName;
    2:optional string displayName;
    3:optional double score;
}

struct StoreQuery{
    1:optional string query;
    2:optional i32 recordNum;
    3:optional i32 imeiNum;
}

struct BrowserQuery{
    1:optional string query;
    2:optional double score;
}

struct SpecialWord{
    1:optional string word;
    2:optional double score;
}

struct AppExt{
    1:optional i64 appId;
    2:optional string packageName;
    3:optional string displayName;
    5:optional string level1CategoryName;
    6:optional string level2CategoryName;
    7:optional list<string> tags;
    8:optional string desc;
    9:optional string brief;
    10:optional i32 developerAppCount;
    11:optional i64 apkSize;
    12:optional i32 star;
    13:optional i64 rankOrder;
    14:optional i64 rankOrderForPad;
    15:optional i32 lastWeekUpdateCount;
    16:optional i32 lastMonthUpdateCount;
    17:optional i32 favoriteCount;
    18:optional i32 feedbackCount;
    19:optional i32 permissionCount;
    20:optional i64 createTime;
    21:optional i32 badCmt;
    22:optional i32 goodCmt;

    30:optional i32 ystdDownload;
    31:optional i32 ystdInstall;
    32:optional i32 ystdBrowser;
    33:optional i32 ystdActive;
    34:optional i32 lastWeekDownload;
    35:optional i32 lastWeekInstall;
    36:optional i32 lastWeekBrowser;
    37:optional i32 lastWeekActive;
    38:optional i32 lastMonthDownload;
    39:optional i32 lastMonthInstall;
    40:optional i32 lastMonthBrowser;
    41:optional i32 lastMonthActive;

    50:optional list<string> displayNameSeg;
    51:optional list<string> descSeg;
    52:optional list<string> briefSeg;
    53:optional list<string> coclickTfIdf;

    60:optional double appActiveRank;
    61:optional double appCdr;
    62:optional double appDownloadRank;
    63:optional double appHot;
    64:optional double appRank;
    65:optional list<string> folderTags;
    66:optional double gameArpu;
    67:optional double gameCdr;
    68:optional double gameRank;
    69:optional double ratingScore;
    70:optional list<string> keywords;
    71:optional list<string> relatedTags;
    72:optional list<string> searchKeywords;
    73:optional string publisher;
    74:optional string wdjCategory;

    80:optional list<Cate> googleCates;
    81:optional list<Cate> emiCates;
    83:optional list<Cate> topics;

    90:optional list<SimilarApp> similarApps;
    91:optional list<StoreQuery> storeQueries;
    92:optional list<BrowserQuery> browserQueries;
    93:optional list<SpecialWord> specialWords;
}

struct QueryExtItem{
    1:optional string query;
    2:optional double weight;
    3:optional list<string> querySeg
}

struct QueryExt{
    1:optional string query;
    2:optional list<QueryExtItem> exts;
    3:optional list<string> extTfIdf;
    4:optional list<string> appLevel1CategoryName;
    5:optional list<string> appLevel2CategoryName;
    6:optional list<string> appTags;
    7:optional list<i64> appIds;
    8:optional list<string> keywords;
    9:optional list<string> relatedTags;
    10:optional list<string> folderTags;
    11:optional list<string> searchKeywords;
    12:optional list<string> displayNames;
    13:optional list<string> publisher;
    14:optional list<i32> googleCates;
}

struct RankInstance{
    1:optional DataRaw data;
    2:optional AppExt app;
    3:optional list<QueryExtItem> queryExts;
    4:optional list<string> queryExtTfIdf;
    5:optional list<string> qyAppLevel1CategoryName;
    6:optional list<string> qyAppLevel2CategoryName;
    7:optional list<string> qyAppTags;
    8:optional list<i64> qyAppIds;
    9:optional list<string> qyKeywords;
    10:optional list<string> qyRelatedTags;
    11:optional list<string> qyFolderTags;
    12:optional list<string> qySearchKeywords;
    13:optional list<string> qyDisplayNames;
    14:optional list<string> qyPublisher;
    15:optional list<i32> googleCates;
}
