/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.schema.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import org.springframework.core.io.Resource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.MimeType;

/**
 * Base class for Apache Avro
 * {@link org.springframework.messaging.converter.MessageConverter} implementations.
 *
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 * @author Sercan Karaoglu
 */
public abstract class AbstractAvroMessageConverter extends AbstractMessageConverter {

	/**
	 * common parser will let user to import external schemas.
	 */
	private Schema.Parser schemaParser = new Schema.Parser();

	protected AbstractAvroMessageConverter(MimeType supportedMimeType) {
		this(Collections.singletonList(supportedMimeType));
	}

	protected AbstractAvroMessageConverter(Collection<MimeType> supportedMimeTypes) {
		super(supportedMimeTypes);
		setContentTypeResolver(new OriginalContentTypeResolver());
	}

	protected Schema parseSchema(Resource r) throws IOException {
		return this.schemaParser.parse(r.getInputStream());
	}

	@Override
	protected boolean canConvertFrom(Message<?> message, Class<?> targetClass) {
		return super.canConvertFrom(message, targetClass)
				&& (message.getPayload() instanceof byte[]);
	}

	@Override
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass,
			Object conversionHint) {
		Object result = null;
		try {
			byte[] payload = (byte[]) message.getPayload();

			MimeType mimeType = getContentTypeResolver().resolve(message.getHeaders());
			if (mimeType == null) {
				if (conversionHint instanceof MimeType) {
					mimeType = (MimeType) conversionHint;
				}
				else {
					return null;
				}
			}

			Schema writerSchema = resolveWriterSchemaForDeserialization(mimeType);
			Schema readerSchema = resolveReaderSchemaForDeserialization(targetClass);

			@SuppressWarnings("unchecked")
			DatumReader<Object> reader = getDatumReader((Class<Object>) targetClass,
					readerSchema, writerSchema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
			result = reader.read(null, decoder);
		}
		catch (IOException e) {
			throw new MessageConversionException(message, "Failed to read payload", e);
		}
		return result;
	}

	private DatumWriter<Object> getDatumWriter(Class<Object> type, Schema schema) {
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected DatumReader<Object> getDatumReader(Class<Object> type, Schema schema,
			Schema writerSchema) {
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
	protected Object convertToInternal(Object payload, MessageHeaders headers,
			Object conversionHint) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			MimeType hintedContentType = null;
			if (conversionHint instanceof MimeType) {
				hintedContentType = (MimeType) conversionHint;
			}
			Schema schema = resolveSchemaForWriting(payload, headers, hintedContentType);
			@SuppressWarnings("unchecked")
			DatumWriter<Object> writer = getDatumWriter(
					(Class<Object>) payload.getClass(), schema);
			Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
			writer.write(payload, encoder);
			encoder.flush();
		}
		catch (IOException e) {
			throw new MessageConversionException("Failed to write payload", e);
		}
		return baos.toByteArray();
	}

	protected abstract Schema resolveSchemaForWriting(Object payload,
			MessageHeaders headers, MimeType hintedContentType);

	protected abstract Schema resolveWriterSchemaForDeserialization(MimeType mimeType);

	protected abstract Schema resolveReaderSchemaForDeserialization(Class<?> targetClass);

}
