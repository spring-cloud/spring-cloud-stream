/*
 * Copyright 2016-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.schema.registry.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.stereotype.Component;

/**
 * Default Concrete implementation of  {@link AvroSchemaServiceManager}.
 *
 * Helps to substitute the default implementation of {@link org.apache.avro.Schema} Generation using Custom Avro
 * schema generator
 *
 * Provide a custom bean definition of {@link AvroSchemaServiceManager} and mark it as @Primary to override this
 * default implementation
 *
 * @author Ish Mahajan
 *
 */

@Component
public class AvroSchemaServiceManagerImpl implements AvroSchemaServiceManager {

	protected final Log logger = LogFactory.getLog(this.getClass());

	/**
	 * get {@link Schema}.
	 * @param clazz {@link Class} for which schema generation is required
	 * @return returns avro schema for given class
	 */
	@Override
	public Schema getSchema(Class<?> clazz) {
		return ReflectData.get().getSchema(clazz);
	}

	/**
	 * get {@link DatumWriter}.
	 * @param type {@link Class} of java object which needs to be serialized
	 * @param schema {@link Schema} of object which needs to be serialized
	 * @return datum writer which can be used to write Avro payload
	 */
	@Override
	public DatumWriter<Object> getDatumWriter(Class<?> type, Schema schema) {
		DatumWriter<Object> writer;
		this.logger.debug("Finding correct DatumWriter for type " + type.getName());
		if (SpecificRecord.class.isAssignableFrom(type)) {
			if (schema != null) {
				writer = new SpecificDatumWriter<>(schema);
			}
			else {
				writer = new SpecificDatumWriter(type);
			}
		}
		else if (GenericRecord.class.isAssignableFrom(type)) {
			writer = new GenericDatumWriter<>(schema);
		}
		else {
			if (schema != null) {
				writer = new ReflectDatumWriter<>(schema);
			}
			else {
				writer = new ReflectDatumWriter(type);
			}
		}
		return writer;
	}

	/**
	 * get {@link DatumReader}.
	 * @param type {@link Class} of java object which needs to be serialized
	 * @param readerSchema {@link Schema} default schema of object which needs to be de-serialized
	 * @param writerSchema {@link Schema} writerSchema provided at run time
	 * @return datum reader which can be used to read Avro payload
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public DatumReader<Object> getDatumReader(Class<?> type, Schema readerSchema, Schema writerSchema) {
		DatumReader<Object> reader = null;
		if (SpecificRecord.class.isAssignableFrom(type)) {
			if (readerSchema != null) {
				if (writerSchema != null) {
					reader = new SpecificDatumReader<>(writerSchema, readerSchema);
				}
				else {
					reader = new SpecificDatumReader<>(readerSchema);
				}
			}
			else {
				reader = new SpecificDatumReader(type);
				if (writerSchema != null) {
					reader.setSchema(writerSchema);
				}
			}
		}
		else if (GenericRecord.class.isAssignableFrom(type)) {
			if (readerSchema != null) {
				if (writerSchema != null) {
					reader = new GenericDatumReader<>(writerSchema, readerSchema);
				}
				else {
					reader = new GenericDatumReader<>(readerSchema);
				}
			}
			else {
				if (writerSchema != null) {
					reader = new GenericDatumReader(writerSchema);
				}
			}
		}
		else {
			reader = new ReflectDatumReader(type);
			if (writerSchema != null) {
				reader.setSchema(writerSchema);
			}
		}
		if (reader == null) {
			throw new MessageConversionException("No schema can be inferred from type "
					+ type.getName() + " and no schema has been explicitly configured.");
		}
		return reader;
	}

	/**
	 * read data from avro type payload {@link DatumReader}.
	 * @param clazz {@link Class} of java object which needs to be serialized
	 * @param payload {@link byte} serialized payload of object which needs to be de-serialized
	 * @param readerSchema {@link Schema} readerSchema of object which needs to be de-serialized
	 * @param writerSchema {@link Schema} writerSchema used to while serializing payload
	 * @return java object after reading Avro Payload
	 * @throws IOException is thrown in case of error
	 */
	@Override
	public Object readData(Class<? extends Object> clazz, byte[] payload, Schema readerSchema, Schema writerSchema)
			throws IOException {
		DatumReader<Object> reader = this.getDatumReader(clazz, readerSchema, writerSchema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
		return reader.read(null, decoder);
	}
}
