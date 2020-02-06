package com.xiaomi.misearch.rank.music.common.model.artist;

import java.io.Serializable;
import java.util.List;
import lombok.Data;
import lombok.AllArgsConstructor;

import com.xiaomi.misearch.rank.music.common.model.Feature;

@Data
@AllArgsConstructor
public class ArtistSample implements Serializable {

  private String query;
  private int qid;
  private int label;
  private ArtistMusicItem artistMusicItem;
  private List<Feature> features;
}
