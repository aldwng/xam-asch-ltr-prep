/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.xiaomi.miui.ad.appstore.feature;

import org.apache.commons.lang.builder.HashCodeBuilder;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class SimilarApp implements TBase<SimilarApp, SimilarApp._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("SimilarApp");

  private static final TField PACKAGE_NAME_FIELD_DESC = new TField("packageName", TType.STRING, (short)1);
  private static final TField DISPLAY_NAME_FIELD_DESC = new TField("displayName", TType.STRING, (short)2);
  private static final TField SCORE_FIELD_DESC = new TField("score", TType.DOUBLE, (short)3);

  private String packageName;
  private String displayName;
  private double score;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    PACKAGE_NAME((short)1, "packageName"),
    DISPLAY_NAME((short)2, "displayName"),
    SCORE((short)3, "score");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // PACKAGE_NAME
          return PACKAGE_NAME;
        case 2: // DISPLAY_NAME
          return DISPLAY_NAME;
        case 3: // SCORE
          return SCORE;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __SCORE_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PACKAGE_NAME, new FieldMetaData("packageName", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.STRING)));
    tmpMap.put(_Fields.DISPLAY_NAME, new FieldMetaData("displayName", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.STRING)));
    tmpMap.put(_Fields.SCORE, new FieldMetaData("score", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.DOUBLE)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(SimilarApp.class, metaDataMap);
  }

  public SimilarApp() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SimilarApp(SimilarApp other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetPackageName()) {
      this.packageName = other.packageName;
    }
    if (other.isSetDisplayName()) {
      this.displayName = other.displayName;
    }
    this.score = other.score;
  }

  public SimilarApp deepCopy() {
    return new SimilarApp(this);
  }

  @Override
  public void clear() {
    this.packageName = null;
    this.displayName = null;
    setScoreIsSet(false);
    this.score = 0.0;
  }

  public String getPackageName() {
    return this.packageName;
  }

  public SimilarApp setPackageName(String packageName) {
    this.packageName = packageName;
    return this;
  }

  public void unsetPackageName() {
    this.packageName = null;
  }

  /** Returns true if field packageName is set (has been asigned a value) and false otherwise */
  public boolean isSetPackageName() {
    return this.packageName != null;
  }

  public void setPackageNameIsSet(boolean value) {
    if (!value) {
      this.packageName = null;
    }
  }

  public String getDisplayName() {
    return this.displayName;
  }

  public SimilarApp setDisplayName(String displayName) {
    this.displayName = displayName;
    return this;
  }

  public void unsetDisplayName() {
    this.displayName = null;
  }

  /** Returns true if field displayName is set (has been asigned a value) and false otherwise */
  public boolean isSetDisplayName() {
    return this.displayName != null;
  }

  public void setDisplayNameIsSet(boolean value) {
    if (!value) {
      this.displayName = null;
    }
  }

  public double getScore() {
    return this.score;
  }

  public SimilarApp setScore(double score) {
    this.score = score;
    setScoreIsSet(true);
    return this;
  }

  public void unsetScore() {
    __isset_bit_vector.clear(__SCORE_ISSET_ID);
  }

  /** Returns true if field score is set (has been asigned a value) and false otherwise */
  public boolean isSetScore() {
    return __isset_bit_vector.get(__SCORE_ISSET_ID);
  }

  public void setScoreIsSet(boolean value) {
    __isset_bit_vector.set(__SCORE_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case PACKAGE_NAME:
      if (value == null) {
        unsetPackageName();
      } else {
        setPackageName((String)value);
      }
      break;

    case DISPLAY_NAME:
      if (value == null) {
        unsetDisplayName();
      } else {
        setDisplayName((String)value);
      }
      break;

    case SCORE:
      if (value == null) {
        unsetScore();
      } else {
        setScore((Double)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case PACKAGE_NAME:
      return getPackageName();

    case DISPLAY_NAME:
      return getDisplayName();

    case SCORE:
      return new Double(getScore());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case PACKAGE_NAME:
      return isSetPackageName();
    case DISPLAY_NAME:
      return isSetDisplayName();
    case SCORE:
      return isSetScore();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SimilarApp)
      return this.equals((SimilarApp)that);
    return false;
  }

  public boolean equals(SimilarApp that) {
    if (that == null)
      return false;

    boolean this_present_packageName = true && this.isSetPackageName();
    boolean that_present_packageName = true && that.isSetPackageName();
    if (this_present_packageName || that_present_packageName) {
      if (!(this_present_packageName && that_present_packageName))
        return false;
      if (!this.packageName.equals(that.packageName))
        return false;
    }

    boolean this_present_displayName = true && this.isSetDisplayName();
    boolean that_present_displayName = true && that.isSetDisplayName();
    if (this_present_displayName || that_present_displayName) {
      if (!(this_present_displayName && that_present_displayName))
        return false;
      if (!this.displayName.equals(that.displayName))
        return false;
    }

    boolean this_present_score = true && this.isSetScore();
    boolean that_present_score = true && that.isSetScore();
    if (this_present_score || that_present_score) {
      if (!(this_present_score && that_present_score))
        return false;
      if (this.score != that.score)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_packageName = true && (isSetPackageName());
    builder.append(present_packageName);
    if (present_packageName)
      builder.append(packageName);

    boolean present_displayName = true && (isSetDisplayName());
    builder.append(present_displayName);
    if (present_displayName)
      builder.append(displayName);

    boolean present_score = true && (isSetScore());
    builder.append(present_score);
    if (present_score)
      builder.append(score);

    return builder.toHashCode();
  }

  public int compareTo(SimilarApp other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    SimilarApp typedOther = (SimilarApp)other;

    lastComparison = Boolean.valueOf(isSetPackageName()).compareTo(typedOther.isSetPackageName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPackageName()) {
      lastComparison = TBaseHelper.compareTo(this.packageName, typedOther.packageName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDisplayName()).compareTo(typedOther.isSetDisplayName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDisplayName()) {
      lastComparison = TBaseHelper.compareTo(this.displayName, typedOther.displayName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetScore()).compareTo(typedOther.isSetScore());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetScore()) {
      lastComparison = TBaseHelper.compareTo(this.score, typedOther.score);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // PACKAGE_NAME
          if (field.type == TType.STRING) {
            this.packageName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // DISPLAY_NAME
          if (field.type == TType.STRING) {
            this.displayName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3: // SCORE
          if (field.type == TType.DOUBLE) {
            this.score = iprot.readDouble();
            setScoreIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.packageName != null) {
      if (isSetPackageName()) {
        oprot.writeFieldBegin(PACKAGE_NAME_FIELD_DESC);
        oprot.writeString(this.packageName);
        oprot.writeFieldEnd();
      }
    }
    if (this.displayName != null) {
      if (isSetDisplayName()) {
        oprot.writeFieldBegin(DISPLAY_NAME_FIELD_DESC);
        oprot.writeString(this.displayName);
        oprot.writeFieldEnd();
      }
    }
    if (isSetScore()) {
      oprot.writeFieldBegin(SCORE_FIELD_DESC);
      oprot.writeDouble(this.score);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SimilarApp(");
    boolean first = true;

    if (isSetPackageName()) {
      sb.append("packageName:");
      if (this.packageName == null) {
        sb.append("null");
      } else {
        sb.append(this.packageName);
      }
      first = false;
    }
    if (isSetDisplayName()) {
      if (!first) sb.append(", ");
      sb.append("displayName:");
      if (this.displayName == null) {
        sb.append("null");
      } else {
        sb.append(this.displayName);
      }
      first = false;
    }
    if (isSetScore()) {
      if (!first) sb.append(", ");
      sb.append("score:");
      sb.append(this.score);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }

}

