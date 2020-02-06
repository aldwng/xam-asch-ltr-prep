package com.xiaomi.misearch.rank.music.common.model.artist;

import java.io.Serializable;
import lombok.Data;

@Data
public class ArtistStoredStatsItem implements Serializable {

  public String id;
  public double songSearchPlayCount;
  public double songSearchSkipCount;
  public double songSearchSkip30sCount;

  public double songArtistSearchPlayCount;
  public double songArtistSearchSkipCount;
  public double songArtistSearchSkip30sCount;

  public double artistSearchFinishRate;
  public double artistSearchFinish30sRate;

  public ArtistStoredStatsItem(ArtistMusicItem artistMusicItem) {
    this.id = artistMusicItem.getId();
    this.songSearchPlayCount = artistMusicItem.getSongSearchPlayCount();
    this.songSearchSkipCount = artistMusicItem.getSongSearchSkipCount();
    this.songSearchSkip30sCount = artistMusicItem.getSongSearchSkip30sCount();
    this.songArtistSearchPlayCount = artistMusicItem.getSongArtistSearchPlayCount();
    this.songArtistSearchSkipCount = artistMusicItem.getSongArtistSearchSkipCount();
    this.songArtistSearchSkip30sCount = artistMusicItem.getSongArtistSearchSkip30sCount();
    this.artistSearchFinishRate = artistMusicItem.getArtistSearchFinishRate();
    this.artistSearchFinish30sRate = artistMusicItem.getArtistSearchFinish30sRate();
  }
}
