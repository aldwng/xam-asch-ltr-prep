package net.xam.asch.ltr.common.model;

import java.io.Serializable;
import java.util.List;

public class MusicItem implements Serializable {
  private String id;

  private double metaRank;
  private double resRank;
  private double quality;
  private double qqSongRawRank;
  private double qqArtistRawRank;
  private double qqRank;
  private double hasLyric;

  private double songSearchPlayCount;
  private double songArtistSearchPlayCount;
  private double songArtistSearchFinishRate;
  private double songArtistSearchCount;

  private double artistSearchCount;
  private double artistMusicCount;
  private double artistOriginCount;

  private List<String> styleTags;

  public MusicItem() { }

  public MusicItem(String id, double metaRank, double resRank, double quality, double qqSongRawRank, double qqArtistRawRank,
                   double qqRank, double hasLyric, double songSearchPlayCount, double songArtistSearchPlayCount,
                   double songArtistSearchFinishRate, double songArtistSearchCount, double artistSearchCount,
                   double artistMusicCount, double artistOriginCount, List<String> styleTags) {
    this.id = id;
    this.metaRank = metaRank;
    this.resRank = resRank;
    this.quality = quality;
    this.qqSongRawRank = qqSongRawRank;
    this.qqArtistRawRank = qqArtistRawRank;
    this.qqRank = qqRank;
    this.hasLyric = hasLyric;
    this.songSearchPlayCount = songSearchPlayCount;
    this.songArtistSearchPlayCount = songArtistSearchPlayCount;
    this.songArtistSearchFinishRate = songArtistSearchFinishRate;
    this.songArtistSearchCount = songArtistSearchCount;
    this.artistSearchCount = artistSearchCount;
    this.artistMusicCount = artistMusicCount;
    this.artistOriginCount = artistOriginCount;
    this.styleTags = styleTags;
  }

  public void setBatchField(StoredMusicItem storedMusicItem) {
    this.songSearchPlayCount = storedMusicItem.songSearchPlayCount;
    this.songArtistSearchPlayCount = storedMusicItem.songArtistSearchPlayCount;
    this.songArtistSearchFinishRate = storedMusicItem.songArtistSearchFinishRate;
    this.songArtistSearchCount = storedMusicItem.songArtistSearchCount;
    this.artistSearchCount = storedMusicItem.artistSearchCount;
    this.artistMusicCount = storedMusicItem.artistMusicCount;
    this.artistOriginCount = storedMusicItem.artistOriginCount;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public double getMetaRank() {
    return metaRank;
  }

  public void setMetaRank(double metaRank) {
    this.metaRank = metaRank;
  }

  public double getResRank() {
    return resRank;
  }

  public void setResRank(double resRank) {
    this.resRank = resRank;
  }

  public double getQuality() {
    return quality;
  }

  public void setQuality(double quality) {
    this.quality = quality;
  }

  public double getQqSongRawRank() {
    return qqSongRawRank;
  }

  public void setQqSongRawRank(double qqSongRawRank) {
    this.qqSongRawRank = qqSongRawRank;
  }

  public double getQqArtistRawRank() {
    return qqArtistRawRank;
  }

  public void setQqArtistRawRank(double qqArtistRawRank) {
    this.qqArtistRawRank = qqArtistRawRank;
  }

  public double getQqRank() {
    return qqRank;
  }

  public void setQqRank(double qqRank) {
    this.qqRank = qqRank;
  }

  public double getHasLyric() {
    return hasLyric;
  }

  public void setHasLyric(double hasLyric) {
    this.hasLyric = hasLyric;
  }

  public double getSongSearchPlayCount() {
    return songSearchPlayCount;
  }

  public void setSongSearchPlayCount(double songSearchPlayCount) {
    this.songSearchPlayCount = songSearchPlayCount;
  }

  public double getSongArtistSearchPlayCount() {
    return songArtistSearchPlayCount;
  }

  public void setSongArtistSearchPlayCount(double songArtistSearchPlayCount) {
    this.songArtistSearchPlayCount = songArtistSearchPlayCount;
  }

  public double getSongArtistSearchFinishRate() {
    return songArtistSearchFinishRate;
  }

  public void setSongArtistSearchFinishRate(double songArtistSearchFinishRate) {
    this.songArtistSearchFinishRate = songArtistSearchFinishRate;
  }

  public double getSongArtistSearchCount() {
    return songArtistSearchCount;
  }

  public void setSongArtistSearchCount(double songArtistSearchCount) {
    this.songArtistSearchCount = songArtistSearchCount;
  }

  public double getArtistSearchCount() {
    return artistSearchCount;
  }

  public void setArtistSearchCount(double artistSearchCount) {
    this.artistSearchCount = artistSearchCount;
  }

  public double getArtistMusicCount() {
    return artistMusicCount;
  }

  public void setArtistMusicCount(double artistMusicCount) {
    this.artistMusicCount = artistMusicCount;
  }

  public double getArtistOriginCount() {
    return artistOriginCount;
  }

  public void setArtistOriginCount(double artistOriginCount) {
    this.artistOriginCount = artistOriginCount;
  }

  public List<String> getStyleTags() {
    return styleTags;
  }

  public void setStyleTags(List<String> styleTags) {
    this.styleTags = styleTags;
  }
}
