package net.xam.ltr.prep.run.model;

import java.io.Serializable;
/**
 * @author aldwang
 */
public class MusicStat implements Serializable {

  // normalized song name, song == query
  private String song;

  private String resourceId;

  private int finishCount;

  private int playCount;

  private int validListenCount;

  // original song name
  private String displaySong;

  private int qqRank = 0;

  public MusicStat() {
  }

  public MusicStat(String song, String resourceId, int finishCount, int playCount, int validListenCount,
                   String displaySong, int qqRank) {
    this.song = song;
    this.resourceId = resourceId;
    this.finishCount = finishCount;
    this.playCount = playCount;
    this.validListenCount = validListenCount;
    this.displaySong = displaySong;
    this.qqRank = qqRank;
  }

  public String getSong() {
    return song;
  }

  public void setSong(String song) {
    this.song = song;
  }

  public String getResourceId() {
    return resourceId;
  }

  public void setResourceId(String resourceId) {
    this.resourceId = resourceId;
  }

  public int getFinishCount() {
    return finishCount;
  }

  public void setFinishCount(int finishCount) {
    this.finishCount = finishCount;
  }

  public int getPlayCount() {
    return playCount;
  }

  public void setPlayCount(int playCount) {
    this.playCount = playCount;
  }

  public int getValidListenCount() {
    return validListenCount;
  }

  public void setValidListenCount(int validListenCount) {
    this.validListenCount = validListenCount;
  }

  public String getDisplaySong() {
    return displaySong;
  }

  public void setDisplaySong(String displaySong) {
    this.displaySong = displaySong;
  }

  public int getQqRank() {
    return qqRank;
  }

  public void setQqRank(int qqRank) {
    this.qqRank = qqRank;
  }
}
