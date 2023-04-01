package net.xam.asch.ltr.common.model;

public enum FeatureName {
  META_RANK("meta_rank"),
  RES_RANK("res_rank"),
  QUALITY("quality"),
  QQ_SONG_RAW_RANK("qq_song_raw_rank"),
  QQ_ARTIST_RAW_RANK("qq_artist_raw_rank"),
  QQ_RANK("qq_rank"),
  HAS_LYRIC("has_lyric"),

  SONG_SEARCH_PLAY_COUNT("song_search_play_count"),
  SONG_ARTIST_SEARCH_PLAY_COUNT("song_artist_search_play_count"),
  SONG_ARTIST_SEARCH_FINISH_RATE("song_artist_search_finish_rate"),
  SONG_ARTIST_SEARCH_COUNT("song_artist_search_count"),

  ARTIST_SEARCH_COUNT("artist_search_count"),
  ARTIST_MUSIC_COUNT("artist_music_count"),
  ARTIST_ORIGIN_COUNT("artist_origin_count");

  private final String name;

  public String getName() {
    return name;
  }

  FeatureName(String name) {
    this.name = name;
  }
}