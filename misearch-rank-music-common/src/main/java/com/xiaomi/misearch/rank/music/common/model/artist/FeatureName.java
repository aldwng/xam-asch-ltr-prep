package com.xiaomi.misearch.rank.music.common.model.artist;

public enum FeatureName {
  SONG_SEARCH_PLAY_COUNT("song_search_play_count"),
  SONG_SEARCH_SKIP_COUNT("song_search_skip_count"),
  SONG_SEARCH_SKIP_30S_COUNT("song_search_skip_30s_count"),
  SONG_SEARCH_FINISH_RATE("song_search_finish_rate"),
  SONG_SEARCH_FINISH_30S_RATE("song_search_finish_30s_rate"),

  SONG_ARTIST_SEARCH_PLAY_COUNT("song_artist_search_play_count"),
  SONG_ARTIST_SEARCH_SKIP_COUNT("song_artist_search_skip_count"),
  SONG_ARTIST_SEARCH_SKIP_30S_COUNT("song_artist_search_skip_30s_count"),
  SONG_ARTIST_SEARCH_FINISH_RATE("song_artist_search_finish_rate"),
  SONG_ARTIST_SEARCH_FINISH_30S_RATE("song_artist_search_finish_30s_rate"),

  ARTIST_SEARCH_FINISH_RATE("artist_search_finish_rate"),
  ARTIST_SEARCH_FINISH_30S_RATE("artist_search_finish_30s_rate"),

  SONG_SEARCH_COUNT("song_search_count"),
  SONG_ARTIST_SEARCH_COUNT("song_artist_search_count"),

  IS_ORIGIN("is_origin");

  private String name;

  public String getName() {
    return name;
  }

  FeatureName(String name) {
    this.name = name;
  }
}