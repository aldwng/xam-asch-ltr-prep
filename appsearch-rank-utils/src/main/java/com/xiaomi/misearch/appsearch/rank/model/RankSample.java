package com.xiaomi.misearch.appsearch.rank.model;

import java.io.Serializable;
import java.util.List;

/**
 * @author Shenglan Wang
 */
public class RankSample implements Serializable {

  private App app;

  private String query;

  private int qid;

  private int label;

  private List<Feature> features;

  public RankSample(App app, String query, int qid, int label,
                    List<Feature> features) {
    this.app = app;
    this.query = query;
    this.qid = qid;
    this.label = label;
    this.features = features;
  }

  public App getApp() {
    return app;
  }

  public void setApp(App app) {
    this.app = app;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public int getQid() {
    return qid;
  }

  public void setQid(int qid) {
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

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("RankSample{");
    sb.append("app=").append(app);
    sb.append(", query='").append(query).append('\'');
    sb.append(", label=").append(label);
    sb.append(", features=").append(features);
    sb.append('}');
    return sb.toString();
  }
}
