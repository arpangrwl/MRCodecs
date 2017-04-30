/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.test.AvroWriteMapReduce;  
@SuppressWarnings("all")
/** Avro storing of for the word count program */
@org.apache.avro.specific.AvroGenerated
public class WordCountSchema extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"WordCountSchema\",\"namespace\":\"com.test.AvroWriteMapReduce\",\"doc\":\"Avro storing of for the word count program\",\"fields\":[{\"name\":\"Word\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Count\",\"type\":[\"null\",\"int\"],\"default\":null}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence Word;
  @Deprecated public java.lang.Integer Count;

  /**
   * Default constructor.
   */
  public WordCountSchema() {}

  /**
   * All-args constructor.
   */
  public WordCountSchema(java.lang.CharSequence Word, java.lang.Integer Count) {
    this.Word = Word;
    this.Count = Count;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return Word;
    case 1: return Count;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: Word = (java.lang.CharSequence)value$; break;
    case 1: Count = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'Word' field.
   */
  public java.lang.CharSequence getWord() {
    return Word;
  }

  /**
   * Sets the value of the 'Word' field.
   * @param value the value to set.
   */
  public void setWord(java.lang.CharSequence value) {
    this.Word = value;
  }

  /**
   * Gets the value of the 'Count' field.
   */
  public java.lang.Integer getCount() {
    return Count;
  }

  /**
   * Sets the value of the 'Count' field.
   * @param value the value to set.
   */
  public void setCount(java.lang.Integer value) {
    this.Count = value;
  }

  /** Creates a new WordCountSchema RecordBuilder */
  public static com.test.AvroWriteMapReduce.WordCountSchema.Builder newBuilder() {
    return new com.test.AvroWriteMapReduce.WordCountSchema.Builder();
  }
  
  /** Creates a new WordCountSchema RecordBuilder by copying an existing Builder */
  public static com.test.AvroWriteMapReduce.WordCountSchema.Builder newBuilder(com.test.AvroWriteMapReduce.WordCountSchema.Builder other) {
    return new com.test.AvroWriteMapReduce.WordCountSchema.Builder(other);
  }
  
  /** Creates a new WordCountSchema RecordBuilder by copying an existing WordCountSchema instance */
  public static com.test.AvroWriteMapReduce.WordCountSchema.Builder newBuilder(com.test.AvroWriteMapReduce.WordCountSchema other) {
    return new com.test.AvroWriteMapReduce.WordCountSchema.Builder(other);
  }
  
  /**
   * RecordBuilder for WordCountSchema instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<WordCountSchema>
    implements org.apache.avro.data.RecordBuilder<WordCountSchema> {

    private java.lang.CharSequence Word;
    private java.lang.Integer Count;

    /** Creates a new Builder */
    private Builder() {
      super(com.test.AvroWriteMapReduce.WordCountSchema.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.test.AvroWriteMapReduce.WordCountSchema.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing WordCountSchema instance */
    private Builder(com.test.AvroWriteMapReduce.WordCountSchema other) {
            super(com.test.AvroWriteMapReduce.WordCountSchema.SCHEMA$);
      if (isValidValue(fields()[0], other.Word)) {
        this.Word = data().deepCopy(fields()[0].schema(), other.Word);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.Count)) {
        this.Count = data().deepCopy(fields()[1].schema(), other.Count);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'Word' field */
    public java.lang.CharSequence getWord() {
      return Word;
    }
    
    /** Sets the value of the 'Word' field */
    public com.test.AvroWriteMapReduce.WordCountSchema.Builder setWord(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.Word = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'Word' field has been set */
    public boolean hasWord() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'Word' field */
    public com.test.AvroWriteMapReduce.WordCountSchema.Builder clearWord() {
      Word = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'Count' field */
    public java.lang.Integer getCount() {
      return Count;
    }
    
    /** Sets the value of the 'Count' field */
    public com.test.AvroWriteMapReduce.WordCountSchema.Builder setCount(java.lang.Integer value) {
      validate(fields()[1], value);
      this.Count = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'Count' field has been set */
    public boolean hasCount() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'Count' field */
    public com.test.AvroWriteMapReduce.WordCountSchema.Builder clearCount() {
      Count = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public WordCountSchema build() {
      try {
        WordCountSchema record = new WordCountSchema();
        record.Word = fieldSetFlags()[0] ? this.Word : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.Count = fieldSetFlags()[1] ? this.Count : (java.lang.Integer) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}