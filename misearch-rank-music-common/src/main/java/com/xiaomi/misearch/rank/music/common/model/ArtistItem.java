package com.xiaomi.misearch.rank.music.common.model;

import java.io.Serializable;

public class ArtistItem implements Serializable {

  private String name;
  private long searchCount;
  private long musicCount;
  private long originCount;

  public ArtistItem(String name, long searchCount, long musicCount, long originCount) {
    this.name = name;
    this.searchCount = searchCount;
    this.musicCount = musicCount;
    this.originCount = originCount;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getSearchCount() {
    return this.searchCount;
  }

  public void setSearchCount(long searchCount) {
    this.searchCount = searchCount;
  }

  public long getMusicCount() {
    return this.musicCount;
  }

  public void setMusicCount(long musicCount) {
    this.musicCount = musicCount;
  }

  public long getOriginCount() {
    return this.originCount;
  }

  public void setOriginCount(long originCount) {
    this.originCount = originCount;
  }
}
