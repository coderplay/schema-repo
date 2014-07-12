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

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import me.tango.schema.repo.SchemaEntry;
import me.tango.schema.repo.SchemaValidationException;
import me.tango.schema.repo.Validator;
import me.tango.schema.repo.ValidatorFactory;

/**
 * An abstraction for {@link Validator} implementations for Avro Schemas.
 */
public abstract class AvroValidator implements Validator {

  @Override
  public final void validate(String schemaToValidate,
      Iterable<SchemaEntry> schemasInOrder) throws SchemaValidationException {
    Schema toValidate = new Schema.Parser().parse(schemaToValidate);
    Iterable<Schema> schemas = new SchemaIterable(schemasInOrder);
    validate(toValidate, schemas);
  }

  protected abstract void validate(Schema toValidate,
      Iterable<Schema> schemasInOrder) throws SchemaValidationException;

  protected void canRead(Schema writtenWith, Schema readUsing)
      throws SchemaValidationException {
    boolean error;
    try {
      error = Symbol.hasErrors(
          new ResolvingGrammarGenerator().generate(writtenWith, readUsing));
    } catch (IOException e) {
      error = true;
    }
    if(error) {
      throw new SchemaValidationException("Cannot read schema:\n"
        + writtenWith.toString(true) + "\nwith:\n"
        + readUsing.toString(true));
    }
  }

  protected void mutualRead(Schema first, Schema second)
      throws SchemaValidationException {
    canRead(first, second);
    canRead(second, first);
  }

  private static final class SchemaIterable implements Iterable<Schema> {
    private final Iterable<SchemaEntry> wrapped;

    private SchemaIterable(Iterable<SchemaEntry> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public Iterator<Schema> iterator() {
      return new SchemaIterator(wrapped.iterator());
    }
  }

  private static final class SchemaIterator implements Iterator<Schema> {
    private final Iterator<SchemaEntry> wrapped;

    private SchemaIterator(Iterator<SchemaEntry> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public boolean hasNext() {
      return wrapped.hasNext();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Schema next() {
      SchemaEntry entry = wrapped.next();
      return new Schema.Parser().parse(entry.getSchema());
    }

  }
  
  public static final String ALL_PRIOR_COMPATIBLE = "avro.all.prior.compatible";
  public static final String ONE_PRIOR_COMPATIBLE = "avro.one.prior.compatible";
  public static final String ALL_PRIOR_CAN_READ = "avro.all.prior.can.read";
  public static final String ONE_PRIOR_CAN_READ = "avro.one.prior.can.read";
  public static final String CAN_READ_ALL_PRIOR = "avro.read.all.prior";
  public static final String CAN_READ_ONE_PRIOR = "avro.read.one.prior";

  public static ValidatorFactory.Builder addValidators(ValidatorFactory.Builder builder) {
    builder.setValidator(CAN_READ_ONE_PRIOR, new ReadOnePrior());
    builder.setValidator(CAN_READ_ALL_PRIOR, new ReadAllPrior());
    builder.setValidator(ONE_PRIOR_CAN_READ, new OnePriorCanRead());
    builder.setValidator(ALL_PRIOR_CAN_READ, new AllPriorCanRead());
    builder.setValidator(ONE_PRIOR_COMPATIBLE, new OnePriorCompatible());
    builder.setValidator(ALL_PRIOR_COMPATIBLE, new AllPriorCompatible());
    return builder;
  }

}
