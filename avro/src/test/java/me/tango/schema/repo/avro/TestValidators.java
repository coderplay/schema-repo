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

import java.util.HashSet;

import me.tango.schema.repo.avro.AvroValidator;
import org.apache.avro.Schema;
import me.tango.schema.repo.InMemoryRepository;
import me.tango.schema.repo.Repository;
import me.tango.schema.repo.SchemaValidationException;
import me.tango.schema.repo.Subject;
import me.tango.schema.repo.SubjectConfig;
import me.tango.schema.repo.ValidatorFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestValidators {
  Repository repo = new InMemoryRepository(
      AvroValidator.addValidators(new ValidatorFactory.Builder()).build());
  Schema rec = new Schema.Parser().parse(
      "{\"type\":\"record\", \"name\":\"test.Rec\", \"fields\":" + 
      "  [" + 
      "    {\"name\":\"a\", \"type\":\"int\", \"default\":1}," + 
      "    {\"name\":\"b\", \"type\":\"long\"}" + 
      "  ]" + 
      "}");
  Schema rec2 = new Schema.Parser().parse(
      "{\"type\":\"record\", \"name\":\"test.Rec\", \"fields\":" + 
      "  [" + 
      "    {\"name\":\"a\", \"type\":\"int\", \"default\":1}," + 
      "    {\"name\":\"b\", \"type\":\"long\"}," + 
      "    {\"name\":\"c\", \"type\":\"int\", \"default\":0}" + 
      "  ]" + 
      "}");
  Schema rec3 = new Schema.Parser().parse(
      "{\"type\":\"record\", \"name\":\"test.Rec\", \"fields\":" + 
      "  [" + 
      "    {\"name\":\"b\", \"type\":\"long\"}," + 
      "    {\"name\":\"c\", \"type\":\"int\", \"default\":0}" + 
      "  ]" + 
      "}");
  Schema rec4 = new Schema.Parser().parse(
      "{\"type\":\"record\", \"name\":\"test.Rec\", \"fields\":" + 
      "  [" + 
      "    {\"name\":\"b\", \"type\":\"long\"}," + 
      "    {\"name\":\"c\", \"type\":\"int\"}" + 
      "  ]" + 
      "}");
  
  @Test
  public void testAllValidators() throws SchemaValidationException {
    HashSet<String> allValidators = new HashSet<String>();
    allValidators.add(AvroValidator.CAN_READ_ALL_PRIOR);
    allValidators.add(AvroValidator.CAN_READ_ONE_PRIOR);
    allValidators.add(AvroValidator.ALL_PRIOR_CAN_READ);
    allValidators.add(AvroValidator.ONE_PRIOR_CAN_READ);
    allValidators.add(AvroValidator.ALL_PRIOR_COMPATIBLE);
    allValidators.add(AvroValidator.ONE_PRIOR_COMPATIBLE);
    Subject sub = repo.register("all", new SubjectConfig.Builder()
      .setValidators(allValidators).build());
    
    // should be able to add the first schema, no matter what:
    
    sub.register(rec.toString());
    
    // this is mutually compatible with the latest, and should pass all
    sub.register(rec2.toString());
    
    // this is mutually compatible with the both, and should pass all
    sub.register(rec3.toString());
  }
  
  @Test
  public void testReadOnePrior() throws SchemaValidationException {
    testValidator(AvroValidator.CAN_READ_ONE_PRIOR, rec4, rec);
  }
  
  @Test
  public void testReadAllPrior() throws SchemaValidationException {
    testValidator(AvroValidator.CAN_READ_ALL_PRIOR, rec4, rec, rec2, rec3);
  }
  
  @Test
  public void testOnePriorCanRead() throws SchemaValidationException {
    testValidator(AvroValidator.ONE_PRIOR_CAN_READ, rec, rec4);
  }
  
  @Test
  public void testAllPriorCanRead() throws SchemaValidationException {
    testValidator(AvroValidator.ALL_PRIOR_CAN_READ, rec, rec4, rec3, rec2);
  }
  
  @Test
  public void testOnePriorCompatible() throws SchemaValidationException {
    testValidator(AvroValidator.ONE_PRIOR_COMPATIBLE, rec, rec4);
  }
  
  @Test
  public void testAllPriorCompatible() throws SchemaValidationException {
    testValidator(AvroValidator.ALL_PRIOR_COMPATIBLE, rec, rec4, rec3, rec2);
  }
  
  private void testValidator(String validatorName,
      Schema schemaFails, Schema... schemaPrep) throws SchemaValidationException {
    Subject sub = repo.register(validatorName, new SubjectConfig.Builder()
        .addValidator(validatorName).build());

    // should be able to add these
    for(Schema prep : schemaPrep) {
      sub.register(prep.toString());
    }
    boolean threw = false;
    try {
      // should fail
      sub.register(schemaFails.toString());
    } catch (SchemaValidationException sve) {
      threw = true;
    }
    Assert.assertTrue(threw);
  }

}
