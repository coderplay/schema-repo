/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package me.tango.schema.repo.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import me.tango.schema.repo.SchemaEntry;
import me.tango.schema.repo.Subject;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;

public class TaggedWriter<T> {
  private final byte[] tag;
  private final DatumWriter<T> writer;
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final BinaryEncoder encoder = 
      EncoderFactory.get().binaryEncoder(baos, null);

  private TaggedWriter(byte[] tag, DatumWriter<T> datumWriter) {
    this.tag = tag;
    this.writer = datumWriter;
  }
  
  public synchronized byte[] encode(T datum) throws IOException {
    // the identifier
    encoder.writeFixed(tag);
    // rest of Avro record
    writer.write(datum, encoder);
    encoder.flush();
    byte[] result = baos.toByteArray();
    baos.reset();
    return result;
  }
  
  public static class Builder {
    private final Subject subject;
    private final Schema schema;
    private final String id;
    public Builder(Subject subject, Schema writerSchema) {
      this.subject = subject;
      this.schema = writerSchema;
      String schemaStr = writerSchema.toString();
      SchemaEntry entry = subject.lookupBySchema(schemaStr);
      if (null == entry) {
        throw new RuntimeException("schema does not exist in subject "
            + subject.getName() + ". Schema:\n" + schemaStr
            + "\nensure the schema has been registered and is compatible");
      }
      this.id = entry.getId();
    }
    
    public <T> TaggedWriter<T> buildSpecific() {
      SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(schema);
      return build(writer);
    }
    
    public <T> TaggedWriter<T> buildGeneric() {
      GenericDatumWriter<T> writer = new GenericDatumWriter<T>(schema);
      return build(writer);
    }
    
    public <T> TaggedWriter<T> buildReflect() {
      ReflectDatumWriter<T> writer = new ReflectDatumWriter<T>(schema);
      return build(writer);
    }
    
    private <T> TaggedWriter<T> build(DatumWriter<T> writer) {
      if(subject.integralKeys()) {
        // get raw bytes for an avro int
        int intId = Integer.valueOf(id);
        byte[] buf = new byte[8];
        int len = BinaryData.encodeInt(intId, new byte[8], 0);
        return new TaggedWriter<T>(Arrays.copyOf(buf, len), writer);
      } else {
        // get raw bytes for an avro string
        byte[] str = Utf8.getBytesFor(id);
        byte[] buf = new byte[8 + str.length];
        int len = BinaryData.encodeInt(str.length, buf, 0);
        System.arraycopy(str, 0, buf, len, str.length);
        byte[] tag = Arrays.copyOf(buf, len + str.length);
        return new TaggedWriter<T>(tag, writer);
      }
    }
  }
  
}
