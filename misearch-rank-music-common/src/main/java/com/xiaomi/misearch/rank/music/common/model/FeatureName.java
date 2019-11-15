package com.xiaomi.misearch.rank.music.common.model;

public enum FeatureName {
  META_RANK(1, "meta_rank"),
  RES_RANK(2, "res_rank"),
  QUALITY(3, "quality"),
  QQ_SONG_RAW_RANK(4, "qq_song_raw_rank"),
  QQ_ARTIST_RAW_RANK(5, "qq_artist_raw_rank"),
  QQ_RANK(6, "qq_rank"),
  HAS_LYRIC(7, "has_lyric"),

  SONG_SEARCH_PLAY_COUNT(8, "song_search_play_count"),
  SONG_ARTIST_SEARCH_PLAY_COUNT(9, "song_artist_search_play_count"),
  SONG_ARTIST_SEARCH_FINISH_RATE(10, "song_artist_search_finish_rate"),
  SONG_ARTIST_SEARCH_COUNT(11, "song_artist_search_count"),

  ARTIST_SEARCH_COUNT(12, "artist_search_count"),
  ARTIST_MUSIC_COUNT(13, "artist_music_count"),
  ARTIST_ORIGIN_COUNT(14, "artist_origin_count");

  private int id;
  private String name;

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  FeatureName(int id, String name) {
    this.id = id;
    this.name = name;
  }
}