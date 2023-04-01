package net.xam.asch.ltr.common.model;

import java.io.Serializable;

public class Feature implements Serializable {

  private String name;
  private int id;
  private double value;

  public Feature(String name, int id, double value) {
    this.name = name;
    this.id = id;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getId() { return id; }

  public void setId(int id) { this.id = id; }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "Feature{" + "name=" + name
           + ", id=" + id
           + ", value=" + value
           + '}';
  }

}
