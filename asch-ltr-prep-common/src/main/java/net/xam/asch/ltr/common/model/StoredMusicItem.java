package net.xam.asch.ltr.common.model;

import java.io.Serializable;

public class StoredMusicItem implements Serializable {

  public String id;

  public double songSearchPlayCount;
  public double songArtistSearchPlayCount;
  public double songArtistSearchFinishRate;
  public double songArtistSearchCount;

  public double artistSearchCount;
  public double artistMusicCount;
  public double artistOriginCount;

  public StoredMusicItem() { }

  public StoredMusicItem(String id, double songSearchPlayCount, double songArtistSearchPlayCount,
                         double songArtistSearchFinishRate, double songArtistSearchCount, double artistSearchCount,
                         double artistMusicCount, double artistOriginCount) {
    this.id = id;
    this.songSearchPlayCount = songSearchPlayCount;
    this.songArtistSearchPlayCount = songArtistSearchPlayCount;
    this.songArtistSearchFinishRate = songArtistSearchFinishRate;
    this.songArtistSearchCount = songArtistSearchCount;
    this.artistSearchCount = artistSearchCount;
    this.artistMusicCount = artistMusicCount;
    this.artistOriginCount = artistOriginCount;
  }

  public StoredMusicItem(MusicItem musicFeature) {
    this.id = musicFeature.getId();
    this.songSearchPlayCount = musicFeature.getSongSearchPlayCount();
    this.songArtistSearchPlayCount = musicFeature.getSongArtistSearchPlayCount();
    this.songArtistSearchFinishRate = musicFeature.getSongArtistSearchFinishRate();
    this.songArtistSearchCount = musicFeature.getSongArtistSearchCount();
    this.artistSearchCount = musicFeature.getArtistSearchCount();
    this.artistMusicCount = musicFeature.getArtistMusicCount();
    this.artistOriginCount = musicFeature.getArtistOriginCount();
  }
}
