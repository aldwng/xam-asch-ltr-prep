package com.xiaomi.misearch.appsearch.rank.model;

/**
 * @author Shenglan Wang
 */
public class Feature {

  private FeatureName name;
  private double value;

  public Feature(FeatureName name, double value) {
    this.name = name;
    this.value = value;
  }

  public FeatureName getName() {
    return name;
  }

  public void setName(FeatureName name) {
    this.name = name;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Feature{");
    sb.append("name=").append(name);
    sb.append(", value=").append(value);
    sb.append('}');
    return sb.toString();
  }
}
