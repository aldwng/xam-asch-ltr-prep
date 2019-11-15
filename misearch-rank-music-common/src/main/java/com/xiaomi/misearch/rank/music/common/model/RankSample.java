package com.xiaomi.misearch.rank.music.common.model;

import java.util.List;

/**
 * @author Shenglan Wang
 */
public class RankSample {

  private MusicItem music;

  private String query;

  private String qid;

  private int label;

  private List<Feature> features;

  public RankSample(MusicItem music, String query, String qid, int label,
                    List<Feature> features) {
    this.music = music;
    this.query = query;
    this.qid = qid;
    this.label = label;
    this.features = features;
  }

  public MusicItem getMusic() {
    return music;
  }

  public void setMusic(MusicItem music) {
    this.music = music;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getQid() {
    return qid;
  }

  public void setQid(String qid) {
    this.qid = qid;
  }

  public int getLabel() {
    return label;
  }

  public void setLabel(int label) {
    this.label = label;
  }

  public List<Feature> getFeatures() {
    return features;
  }

  public void setFeatures(List<Feature> features) {
    this.features = features;
  }
}

