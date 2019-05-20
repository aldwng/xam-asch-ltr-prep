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

public class SpecialWord implements TBase<SpecialWord, SpecialWord._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("SpecialWord");

  private static final TField WORD_FIELD_DESC = new TField("word", TType.STRING, (short)1);
  private static final TField SCORE_FIELD_DESC = new TField("score", TType.DOUBLE, (short)2);

  private String word;
  private double score;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    WORD((short)1, "word"),
    SCORE((short)2, "score");

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
        case 1: // WORD
          return WORD;
        case 2: // SCORE
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
    tmpMap.put(_Fields.WORD, new FieldMetaData("word", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.STRING)));
    tmpMap.put(_Fields.SCORE, new FieldMetaData("score", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.DOUBLE)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(SpecialWord.class, metaDataMap);
  }

  public SpecialWord() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SpecialWord(SpecialWord other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetWord()) {
      this.word = other.word;
    }
    this.score = other.score;
  }

  public SpecialWord deepCopy() {
    return new SpecialWord(this);
  }

  @Override
  public void clear() {
    this.word = null;
    setScoreIsSet(false);
    this.score = 0.0;
  }

  public String getWord() {
    return this.word;
  }

  public SpecialWord setWord(String word) {
    this.word = word;
    return this;
  }

  public void unsetWord() {
    this.word = null;
  }

  /** Returns true if field word is set (has been asigned a value) and false otherwise */
  public boolean isSetWord() {
    return this.word != null;
  }

  public void setWordIsSet(boolean value) {
    if (!value) {
      this.word = null;
    }
  }

  public double getScore() {
    return this.score;
  }

  public SpecialWord setScore(double score) {
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
    case WORD:
      if (value == null) {
        unsetWord();
      } else {
        setWord((String)value);
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
    case WORD:
      return getWord();

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
    case WORD:
      return isSetWord();
    case SCORE:
      return isSetScore();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SpecialWord)
      return this.equals((SpecialWord)that);
    return false;
  }

  public boolean equals(SpecialWord that) {
    if (that == null)
      return false;

    boolean this_present_word = true && this.isSetWord();
    boolean that_present_word = true && that.isSetWord();
    if (this_present_word || that_present_word) {
      if (!(this_present_word && that_present_word))
        return false;
      if (!this.word.equals(that.word))
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

    boolean present_word = true && (isSetWord());
    builder.append(present_word);
    if (present_word)
      builder.append(word);

    boolean present_score = true && (isSetScore());
    builder.append(present_score);
    if (present_score)
      builder.append(score);

    return builder.toHashCode();
  }

  public int compareTo(SpecialWord other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    SpecialWord typedOther = (SpecialWord)other;

    lastComparison = Boolean.valueOf(isSetWord()).compareTo(typedOther.isSetWord());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetWord()) {
      lastComparison = TBaseHelper.compareTo(this.word, typedOther.word);
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
        case 1: // WORD
          if (field.type == TType.STRING) {
            this.word = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // SCORE
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
    if (this.word != null) {
      if (isSetWord()) {
        oprot.writeFieldBegin(WORD_FIELD_DESC);
        oprot.writeString(this.word);
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
    StringBuilder sb = new StringBuilder("SpecialWord(");
    boolean first = true;

    if (isSetWord()) {
      sb.append("word:");
      if (this.word == null) {
        sb.append("null");
      } else {
        sb.append(this.word);
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

