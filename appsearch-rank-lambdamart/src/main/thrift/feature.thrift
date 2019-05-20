namespace java com.xiaomi.miui.ad.appstore.feature

struct BaseFea {
  1: optional i64 id;
  2: optional string fea;
  3: optional double value;
}

struct Sample {
  1: optional i32 label;
  2: optional i64 qid;
  3: optional string query;
  4: optional list<BaseFea> features;
  5: optional string common;
}

struct FeaMap{
    1:optional i64 id,
    2:optional string fea
}

struct QueryMap{
    1:optional i64 id,
    2:optional string query
}


