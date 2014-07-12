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

import java.util.Iterator;

import org.apache.avro.Schema;
import me.tango.schema.repo.SchemaValidationException;
import me.tango.schema.repo.Subject;
import me.tango.schema.repo.Validator;

/**
 * An abstraction for {@link Validator} implementations for Avro Schemas.
 * This abstraction calls {@link #validate(Schema, Schema)} for every
 * schema in the {@link Subject}, in order from most recent to oldest.
 */
public abstract class AvroAllValidator extends AvroValidator {

  protected final void validate(Schema toValidate, Iterable<Schema> schemasInOrder)
      throws SchemaValidationException {
    Iterator<Schema> schemas = schemasInOrder.iterator();
    while(schemas.hasNext()) {
      Schema existing = schemas.next();
      validate(toValidate, existing);
    }
  }

  protected abstract void validate(Schema toValidate, Schema existing) 
      throws SchemaValidationException;
  
}
