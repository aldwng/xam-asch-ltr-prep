package net.xam.asch.ltr.common.model.artist;

import java.io.Serializable;
import lombok.Data;

@Data
public class ArtistMusicItem implements Serializable {
  private String id;

  private double songSearchPlayCount;
  private double songSearchSkipCount;
  private double songSearchSkip30sCount;
  private double songSearchFinishRate;
  private double songSearchFinish30sRate;

  private double songArtistSearchPlayCount;
  private double songArtistSearchSkipCount;
  private double songArtistSearchSkip30sCount;
  private double songArtistSearchFinishRate;
  private double songArtistSearchFinish30sRate;

  private double artistSearchFinishRate;
  private double artistSearchFinish30sRate;

  private double songSearchCount;
  private double songArtistSearchCount;

  private boolean origin;

  private String songName;
  private String artistName;
  private boolean exposedArtistSearch;

  private double songSearchLabelFinishRate;
  private double songArtistSearchLabelFinishRate;
  private double artistSearchLabelFinishRate;

  public ArtistMusicItem() {
  }

  public ArtistMusicItem(String id, double songSearchPlayCount, double songSearchFinishCount, double songSearchFinish30sCount,
                         double songArtistSearchPlayCount, double songArtistSearchFinishCount, double songArtistSearchFinish30sCount,
                         double artistSearchPlayCount, double artistSearchFinishCount, double artistSearchFinish30sCount) {
    this.id = id;
    this.songSearchPlayCount = songSearchPlayCount;
    this.songSearchSkipCount = songSearchPlayCount - songSearchFinishCount;
    this.songSearchSkip30sCount = songSearchPlayCount - songSearchFinish30sCount;
    this.songArtistSearchPlayCount = songArtistSearchPlayCount;
    this.songArtistSearchSkipCount = songArtistSearchPlayCount - songArtistSearchFinishCount;
    this.songArtistSearchSkip30sCount = songArtistSearchPlayCount - songArtistSearchFinish30sCount;
    this.songSearchFinishRate = calcSongSearchFinishRate();
    this.songSearchFinish30sRate = calcSongSearchFinish30sRate();
    this.songArtistSearchFinishRate = calcSongArtistSearchFinishRate();
    this.songArtistSearchFinish30sRate = calcSongArtistSearchFinish30sRate();
    this.artistSearchFinishRate = calcArtistSearchFinishRate(artistSearchPlayCount, artistSearchFinishCount);
    this.artistSearchFinish30sRate = calcArtistSearchFinish30sRate(artistSearchPlayCount, artistSearchFinish30sCount);
    this.songSearchLabelFinishRate = calcSongSearchLabelFinishRate();
    this.songArtistSearchLabelFinishRate = calcSongArtistSearchLabelFinishRate();
    this.artistSearchLabelFinishRate = calcArtistSearchLabelFinishRate();
    if (artistSearchPlayCount > 5) {
      this.exposedArtistSearch = true;
    }
  }

  public ArtistMusicItem(ArtistStoredStatsItem storedStatsItem, long songSearchCount, long songArtistSearchCount) {
    this.id = storedStatsItem.id;
    this.songSearchPlayCount = storedStatsItem.getSongSearchPlayCount();
    this.songSearchSkipCount = storedStatsItem.getSongSearchSkipCount();
    this.songSearchSkip30sCount = storedStatsItem.getSongSearchSkip30sCount();
    this.songArtistSearchPlayCount = storedStatsItem.getSongArtistSearchPlayCount();
    this.songArtistSearchSkipCount = storedStatsItem.getSongArtistSearchSkipCount();
    this.songArtistSearchSkip30sCount = storedStatsItem.getSongArtistSearchSkip30sCount();
    this.artistSearchFinishRate = storedStatsItem.getArtistSearchFinishRate();
    this.artistSearchFinish30sRate = storedStatsItem.getArtistSearchFinish30sRate();
    this.songSearchCount = (double)songSearchCount;
    this.songArtistSearchCount = (double)songArtistSearchCount;
    this.songSearchFinishRate = calcSongSearchFinishRate();
    this.songSearchFinish30sRate = calcSongSearchFinish30sRate();
    this.songArtistSearchFinishRate = calcSongArtistSearchFinishRate();
    this.songArtistSearchFinish30sRate = calcSongArtistSearchFinish30sRate();
  }

  private double calcSongSearchFinishRate() {
    if (this.songSearchPlayCount > 50) {
      return (this.songSearchPlayCount - this.songSearchSkipCount) / this.songSearchPlayCount;
    }
    return 0D;
  }

  private double calcSongSearchFinish30sRate() {
    if (this.songSearchPlayCount > 50) {
      return (this.songSearchPlayCount - this.songSearchSkip30sCount) / this.songSearchPlayCount;
    }
    return 0D;
  }

  private double calcSongArtistSearchFinishRate() {
    if (this.songArtistSearchPlayCount > 20) {
      return (this.songArtistSearchPlayCount - this.songArtistSearchSkipCount) / this.songArtistSearchPlayCount;
    }
    return 0D;
  }

  private double calcSongArtistSearchFinish30sRate() {
    if (this.songArtistSearchPlayCount > 20) {
      return (this.songArtistSearchPlayCount - this.songArtistSearchSkip30sCount) / this.songArtistSearchPlayCount;
    }
    return 0D;
  }

  private double calcArtistSearchFinishRate(double artistSearchPlayCount, double artistSearchFinishCount) {
    if (artistSearchPlayCount > 20) {
      return artistSearchFinishCount / artistSearchPlayCount;
    }
    return 0D;
  }

  private double calcArtistSearchFinish30sRate(double artistSearchPlayCount, double artistSearchFinish30sCount) {
    if (artistSearchPlayCount > 20) {
      return artistSearchFinish30sCount / artistSearchPlayCount;
    }
    return 0D;
  }

  private double calcSongSearchLabelFinishRate() {
    return this.songSearchFinishRate * 0.5 + this.songSearchFinish30sRate * 0.5;
  }

  private double calcSongArtistSearchLabelFinishRate() {
    return this.songArtistSearchFinishRate * 0.5 + this.songArtistSearchFinish30sRate * 0.5;
  }

  private double calcArtistSearchLabelFinishRate() {
    return this.artistSearchFinishRate * 0.5 + artistSearchFinish30sRate * 0.5;
  }
}
