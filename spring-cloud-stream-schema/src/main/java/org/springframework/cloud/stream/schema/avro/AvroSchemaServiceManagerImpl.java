/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.schema.avro;

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


@Component
public class AvroSchemaServiceManagerImpl implements AvroSchemaServiceManager {

	protected final Log logger = LogFactory.getLog(this.getClass());

	@Override
	public Schema getSchema(Class<?> clazz) {
		return ReflectData.get().getSchema(clazz);
	}

	@Override
	public DatumWriter<Object> getDatumWriter(Class<Object> type, Schema schema) {
		DatumWriter<Object> writer;
		this.logger.debug("Finding correct DatumWriter for type " + type.getName());
		if (SpecificRecord.class.isAssignableFrom(type)) {
			if (schema != null) {
				writer = new SpecificDatumWriter<>(schema);
			}
			else {
				writer = new SpecificDatumWriter<>(type);
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
				writer = new ReflectDatumWriter<>(type);
			}
		}
		return writer;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public DatumReader<Object> getDatumReader(Class<Object> type, Schema schema, Schema writerSchema) {
		DatumReader<Object> reader = null;
		if (SpecificRecord.class.isAssignableFrom(type)) {
			if (schema != null) {
				if (writerSchema != null) {
					reader = new SpecificDatumReader<>(writerSchema, schema);
				}
				else {
					reader = new SpecificDatumReader<>(schema);
				}
			}
			else {
				reader = new SpecificDatumReader<>(type);
				if (writerSchema != null) {
					reader.setSchema(writerSchema);
				}
			}
		}
		else if (GenericRecord.class.isAssignableFrom(type)) {
			if (schema != null) {
				if (writerSchema != null) {
					reader = new GenericDatumReader<>(writerSchema, schema);
				}
				else {
					reader = new GenericDatumReader<>(schema);
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

	@Override
	public Object readData(Class<?> clazz, byte[] payload, Schema readerSchema, Schema writerSchema) throws IOException {
		DatumReader<Object> reader = this.getDatumReader((Class<Object>) clazz,
			readerSchema, writerSchema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
		return reader.read(null, decoder);
	}
}
