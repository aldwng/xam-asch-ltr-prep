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

public class Sample implements TBase<Sample, Sample._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("Sample");

  private static final TField LABEL_FIELD_DESC = new TField("label", TType.I32, (short)1);
  private static final TField QID_FIELD_DESC = new TField("qid", TType.I64, (short)2);
  private static final TField QUERY_FIELD_DESC = new TField("query", TType.STRING, (short)3);
  private static final TField FEATURES_FIELD_DESC = new TField("features", TType.LIST, (short)4);
  private static final TField COMMON_FIELD_DESC = new TField("common", TType.STRING, (short)5);

  private int label;
  private long qid;
  private String query;
  private List<BaseFea> features;
  private String common;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    LABEL((short)1, "label"),
    QID((short)2, "qid"),
    QUERY((short)3, "query"),
    FEATURES((short)4, "features"),
    COMMON((short)5, "common");

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
        case 1: // LABEL
          return LABEL;
        case 2: // QID
          return QID;
        case 3: // QUERY
          return QUERY;
        case 4: // FEATURES
          return FEATURES;
        case 5: // COMMON
          return COMMON;
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
  private static final int __LABEL_ISSET_ID = 0;
  private static final int __QID_ISSET_ID = 1;
  private BitSet __isset_bit_vector = new BitSet(2);

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.LABEL, new FieldMetaData("label", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.I32)));
    tmpMap.put(_Fields.QID, new FieldMetaData("qid", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.I64)));
    tmpMap.put(_Fields.QUERY, new FieldMetaData("query", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.STRING)));
    tmpMap.put(_Fields.FEATURES, new FieldMetaData("features", TFieldRequirementType.OPTIONAL, 
        new ListMetaData(TType.LIST, 
            new StructMetaData(TType.STRUCT, BaseFea.class))));
    tmpMap.put(_Fields.COMMON, new FieldMetaData("common", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(Sample.class, metaDataMap);
  }

  public Sample() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Sample(Sample other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.label = other.label;
    this.qid = other.qid;
    if (other.isSetQuery()) {
      this.query = other.query;
    }
    if (other.isSetFeatures()) {
      List<BaseFea> __this__features = new ArrayList<BaseFea>();
      for (BaseFea other_element : other.features) {
        __this__features.add(new BaseFea(other_element));
      }
      this.features = __this__features;
    }
    if (other.isSetCommon()) {
      this.common = other.common;
    }
  }

  public Sample deepCopy() {
    return new Sample(this);
  }

  @Override
  public void clear() {
    setLabelIsSet(false);
    this.label = 0;
    setQidIsSet(false);
    this.qid = 0;
    this.query = null;
    this.features = null;
    this.common = null;
  }

  public int getLabel() {
    return this.label;
  }

  public Sample setLabel(int label) {
    this.label = label;
    setLabelIsSet(true);
    return this;
  }

  public void unsetLabel() {
    __isset_bit_vector.clear(__LABEL_ISSET_ID);
  }

  /** Returns true if field label is set (has been asigned a value) and false otherwise */
  public boolean isSetLabel() {
    return __isset_bit_vector.get(__LABEL_ISSET_ID);
  }

  public void setLabelIsSet(boolean value) {
    __isset_bit_vector.set(__LABEL_ISSET_ID, value);
  }

  public long getQid() {
    return this.qid;
  }

  public Sample setQid(long qid) {
    this.qid = qid;
    setQidIsSet(true);
    return this;
  }

  public void unsetQid() {
    __isset_bit_vector.clear(__QID_ISSET_ID);
  }

  /** Returns true if field qid is set (has been asigned a value) and false otherwise */
  public boolean isSetQid() {
    return __isset_bit_vector.get(__QID_ISSET_ID);
  }

  public void setQidIsSet(boolean value) {
    __isset_bit_vector.set(__QID_ISSET_ID, value);
  }

  public String getQuery() {
    return this.query;
  }

  public Sample setQuery(String query) {
    this.query = query;
    return this;
  }

  public void unsetQuery() {
    this.query = null;
  }

  /** Returns true if field query is set (has been asigned a value) and false otherwise */
  public boolean isSetQuery() {
    return this.query != null;
  }

  public void setQueryIsSet(boolean value) {
    if (!value) {
      this.query = null;
    }
  }

  public int getFeaturesSize() {
    return (this.features == null) ? 0 : this.features.size();
  }

  public java.util.Iterator<BaseFea> getFeaturesIterator() {
    return (this.features == null) ? null : this.features.iterator();
  }

  public void addToFeatures(BaseFea elem) {
    if (this.features == null) {
      this.features = new ArrayList<BaseFea>();
    }
    this.features.add(elem);
  }

  public List<BaseFea> getFeatures() {
    return this.features;
  }

  public Sample setFeatures(List<BaseFea> features) {
    this.features = features;
    return this;
  }

  public void unsetFeatures() {
    this.features = null;
  }

  /** Returns true if field features is set (has been asigned a value) and false otherwise */
  public boolean isSetFeatures() {
    return this.features != null;
  }

  public void setFeaturesIsSet(boolean value) {
    if (!value) {
      this.features = null;
    }
  }

  public String getCommon() {
    return this.common;
  }

  public Sample setCommon(String common) {
    this.common = common;
    return this;
  }

  public void unsetCommon() {
    this.common = null;
  }

  /** Returns true if field common is set (has been asigned a value) and false otherwise */
  public boolean isSetCommon() {
    return this.common != null;
  }

  public void setCommonIsSet(boolean value) {
    if (!value) {
      this.common = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case LABEL:
      if (value == null) {
        unsetLabel();
      } else {
        setLabel((Integer)value);
      }
      break;

    case QID:
      if (value == null) {
        unsetQid();
      } else {
        setQid((Long)value);
      }
      break;

    case QUERY:
      if (value == null) {
        unsetQuery();
      } else {
        setQuery((String)value);
      }
      break;

    case FEATURES:
      if (value == null) {
        unsetFeatures();
      } else {
        setFeatures((List<BaseFea>)value);
      }
      break;

    case COMMON:
      if (value == null) {
        unsetCommon();
      } else {
        setCommon((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case LABEL:
      return new Integer(getLabel());

    case QID:
      return new Long(getQid());

    case QUERY:
      return getQuery();

    case FEATURES:
      return getFeatures();

    case COMMON:
      return getCommon();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case LABEL:
      return isSetLabel();
    case QID:
      return isSetQid();
    case QUERY:
      return isSetQuery();
    case FEATURES:
      return isSetFeatures();
    case COMMON:
      return isSetCommon();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Sample)
      return this.equals((Sample)that);
    return false;
  }

  public boolean equals(Sample that) {
    if (that == null)
      return false;

    boolean this_present_label = true && this.isSetLabel();
    boolean that_present_label = true && that.isSetLabel();
    if (this_present_label || that_present_label) {
      if (!(this_present_label && that_present_label))
        return false;
      if (this.label != that.label)
        return false;
    }

    boolean this_present_qid = true && this.isSetQid();
    boolean that_present_qid = true && that.isSetQid();
    if (this_present_qid || that_present_qid) {
      if (!(this_present_qid && that_present_qid))
        return false;
      if (this.qid != that.qid)
        return false;
    }

    boolean this_present_query = true && this.isSetQuery();
    boolean that_present_query = true && that.isSetQuery();
    if (this_present_query || that_present_query) {
      if (!(this_present_query && that_present_query))
        return false;
      if (!this.query.equals(that.query))
        return false;
    }

    boolean this_present_features = true && this.isSetFeatures();
    boolean that_present_features = true && that.isSetFeatures();
    if (this_present_features || that_present_features) {
      if (!(this_present_features && that_present_features))
        return false;
      if (!this.features.equals(that.features))
        return false;
    }

    boolean this_present_common = true && this.isSetCommon();
    boolean that_present_common = true && that.isSetCommon();
    if (this_present_common || that_present_common) {
      if (!(this_present_common && that_present_common))
        return false;
      if (!this.common.equals(that.common))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_label = true && (isSetLabel());
    builder.append(present_label);
    if (present_label)
      builder.append(label);

    boolean present_qid = true && (isSetQid());
    builder.append(present_qid);
    if (present_qid)
      builder.append(qid);

    boolean present_query = true && (isSetQuery());
    builder.append(present_query);
    if (present_query)
      builder.append(query);

    boolean present_features = true && (isSetFeatures());
    builder.append(present_features);
    if (present_features)
      builder.append(features);

    boolean present_common = true && (isSetCommon());
    builder.append(present_common);
    if (present_common)
      builder.append(common);

    return builder.toHashCode();
  }

  public int compareTo(Sample other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Sample typedOther = (Sample)other;

    lastComparison = Boolean.valueOf(isSetLabel()).compareTo(typedOther.isSetLabel());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLabel()) {
      lastComparison = TBaseHelper.compareTo(this.label, typedOther.label);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetQid()).compareTo(typedOther.isSetQid());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQid()) {
      lastComparison = TBaseHelper.compareTo(this.qid, typedOther.qid);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetQuery()).compareTo(typedOther.isSetQuery());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQuery()) {
      lastComparison = TBaseHelper.compareTo(this.query, typedOther.query);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetFeatures()).compareTo(typedOther.isSetFeatures());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFeatures()) {
      lastComparison = TBaseHelper.compareTo(this.features, typedOther.features);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCommon()).compareTo(typedOther.isSetCommon());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCommon()) {
      lastComparison = TBaseHelper.compareTo(this.common, typedOther.common);
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
        case 1: // LABEL
          if (field.type == TType.I32) {
            this.label = iprot.readI32();
            setLabelIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // QID
          if (field.type == TType.I64) {
            this.qid = iprot.readI64();
            setQidIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3: // QUERY
          if (field.type == TType.STRING) {
            this.query = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 4: // FEATURES
          if (field.type == TType.LIST) {
            {
              TList _list0 = iprot.readListBegin();
              this.features = new ArrayList<BaseFea>(_list0.size);
              for (int _i1 = 0; _i1 < _list0.size; ++_i1)
              {
                BaseFea _elem2;
                _elem2 = new BaseFea();
                _elem2.read(iprot);
                this.features.add(_elem2);
              }
              iprot.readListEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 5: // COMMON
          if (field.type == TType.STRING) {
            this.common = iprot.readString();
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
    if (isSetLabel()) {
      oprot.writeFieldBegin(LABEL_FIELD_DESC);
      oprot.writeI32(this.label);
      oprot.writeFieldEnd();
    }
    if (isSetQid()) {
      oprot.writeFieldBegin(QID_FIELD_DESC);
      oprot.writeI64(this.qid);
      oprot.writeFieldEnd();
    }
    if (this.query != null) {
      if (isSetQuery()) {
        oprot.writeFieldBegin(QUERY_FIELD_DESC);
        oprot.writeString(this.query);
        oprot.writeFieldEnd();
      }
    }
    if (this.features != null) {
      if (isSetFeatures()) {
        oprot.writeFieldBegin(FEATURES_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.features.size()));
          for (BaseFea _iter3 : this.features)
          {
            _iter3.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
    }
    if (this.common != null) {
      if (isSetCommon()) {
        oprot.writeFieldBegin(COMMON_FIELD_DESC);
        oprot.writeString(this.common);
        oprot.writeFieldEnd();
      }
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Sample(");
    boolean first = true;

    if (isSetLabel()) {
      sb.append("label:");
      sb.append(this.label);
      first = false;
    }
    if (isSetQid()) {
      if (!first) sb.append(", ");
      sb.append("qid:");
      sb.append(this.qid);
      first = false;
    }
    if (isSetQuery()) {
      if (!first) sb.append(", ");
      sb.append("query:");
      if (this.query == null) {
        sb.append("null");
      } else {
        sb.append(this.query);
      }
      first = false;
    }
    if (isSetFeatures()) {
      if (!first) sb.append(", ");
      sb.append("features:");
      if (this.features == null) {
        sb.append("null");
      } else {
        sb.append(this.features);
      }
      first = false;
    }
    if (isSetCommon()) {
      if (!first) sb.append(", ");
      sb.append("common:");
      if (this.common == null) {
        sb.append("null");
      } else {
        sb.append(this.common);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }

}
