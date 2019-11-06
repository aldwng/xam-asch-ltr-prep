package com.xiaomi.misearch.rank.appstore.common.model;

import java.io.Serializable;
import java.util.List;

/**
 * @author Shenglan Wang
 */
public class App implements Serializable {

  private String id;
  private String rawId;
  private String packageName;
  private String displayName;
  private String brief;
  private List<String> keywords;
  private String level1Category;
  private String level2Category;
  private double appActiveRank;
  private double appCdr;
  private double appDownloadRank;
  private double appRank;

  public App() {
  }

  public App(String id, String rawId, String packageName, String displayName, String brief,
             List<String> keywords, String level1Category, String level2Category, double appActiveRank, double appCdr,
             double appDownloadRank, double appRank) {
    this.id = id;
    this.rawId = rawId;
    this.packageName = packageName;
    this.displayName = displayName;
    this.brief = brief;
    this.keywords = keywords;
    this.level1Category = level1Category;
    this.level2Category = level2Category;
    this.appActiveRank = appActiveRank;
    this.appCdr = appCdr;
    this.appDownloadRank = appDownloadRank;
    this.appRank = appRank;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getRawId() {
    return rawId;
  }

  public void setRawId(String rawId) {
    this.rawId = rawId;
  }

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

  public String getBrief() {
    return brief;
  }

  public void setBrief(String brief) {
    this.brief = brief;
  }

  public List<String> getKeywords() {
    return keywords;
  }

  public void setKeywords(List<String> keywords) {
    this.keywords = keywords;
  }

  public String getLevel1Category() {
    return level1Category;
  }

  public void setLevel1Category(String level1Category) {
    this.level1Category = level1Category;
  }

  public String getLevel2Category() {
    return level2Category;
  }

  public void setLevel2Category(String level2Category) {
    this.level2Category = level2Category;
  }

  public double getAppActiveRank() {
    return appActiveRank;
  }

  public void setAppActiveRank(double appActiveRank) {
    this.appActiveRank = appActiveRank;
  }

  public double getAppCdr() {
    return appCdr;
  }

  public void setAppCdr(double appCdr) {
    this.appCdr = appCdr;
  }

  public double getAppDownloadRank() {
    return appDownloadRank;
  }

  public void setAppDownloadRank(double appDownloadRank) {
    this.appDownloadRank = appDownloadRank;
  }

  public double getAppRank() {
    return appRank;
  }

  public void setAppRank(double appRank) {
    this.appRank = appRank;
  }
}
