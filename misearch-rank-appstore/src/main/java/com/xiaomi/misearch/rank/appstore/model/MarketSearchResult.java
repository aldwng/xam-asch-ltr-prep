package com.xiaomi.misearch.rank.appstore.model;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Shenglan Wang
 */
public class MarketSearchResult implements Serializable {

  public enum Market {
    tencent, wdj, baidu
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class App implements Serializable {

    private String packageName;
    private String displayName;
    private String downCount;
    private String brief;
    private String introduction;
    private String category;
    private String publisher;
    private String averageRating;
    private String ratingCount;

    public String getPackageName() {
      return packageName;
    }

    public void setPackageName(String packageName) {
      this.packageName = packageName;
    }

    public String getDisplayName() {
      return displayName;
    }

    public void setDisplayName(String displayName) {
      this.displayName = displayName;
    }

    public String getDownCount() {
      return downCount;
    }

    public void setDownCount(String downCount) {
      this.downCount = downCount;
    }

    public String getBrief() {
      return brief;
    }

    public void setBrief(String brief) {
      this.brief = brief;
    }

    public String getIntroduction() {
      return introduction;
    }

    public void setIntroduction(String introduction) {
      this.introduction = introduction;
    }

    public String getCategory() {
      return category;
    }

    public void setCategory(String category) {
      this.category = category;
    }

    public String getPublisher() {
      return publisher;
    }

    public void setPublisher(String publisher) {
      this.publisher = publisher;
    }

    public String getAverageRating() {
      return averageRating;
    }

    public void setAverageRating(String averageRating) {
      this.averageRating = averageRating;
    }

    public String getRatingCount() {
      return ratingCount;
    }

    public void setRatingCount(String ratingCount) {
      this.ratingCount = ratingCount;
    }
  }

  private Market market;
  private String query;
  private List<App> result;

  public Market getMarket() {
    return market;
  }

  public void setMarket(Market market) {
    this.market = market;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public List<App> getResult() {
    return result;
  }

  public void setResult(List<App> result) {
    this.result = result;
  }
}
