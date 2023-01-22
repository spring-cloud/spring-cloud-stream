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

package org.springframework.cloud.stream.schema.registry.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

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
 * @author Ish Mahajan
 */
public abstract class AbstractAvroMessageConverter extends AbstractMessageConverter {

	/**
	 * common parser will let user to import external schemas.
	 */
	private Schema.Parser schemaParser = new Schema.Parser();
	private AvroSchemaServiceManager avroSchemaServiceManager;

	@Deprecated
	protected AbstractAvroMessageConverter(MimeType supportedMimeType) {
		this(Collections.singletonList(supportedMimeType), new AvroSchemaServiceManagerImpl());
	}

	protected AbstractAvroMessageConverter(MimeType supportedMimeType, AvroSchemaServiceManager avroSchemaServiceManager) {
		this(Collections.singletonList(supportedMimeType), avroSchemaServiceManager);
	}

	@Deprecated
	protected AbstractAvroMessageConverter(Collection<MimeType> supportedMimeTypes) {
		this(supportedMimeTypes, new AvroSchemaServiceManagerImpl());
		setContentTypeResolver(new OriginalContentTypeResolver());
	}

	protected AbstractAvroMessageConverter(Collection<MimeType> supportedMimeTypes, AvroSchemaServiceManager manager) {
		super(supportedMimeTypes);
		setContentTypeResolver(new OriginalContentTypeResolver());
		this.avroSchemaServiceManager = manager;
	}

	protected AvroSchemaServiceManager avroSchemaServiceManager() {
		return this.avroSchemaServiceManager;
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
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
		Object result;
		try {
			byte[] payload = (byte[]) message.getPayload();

			MimeType mimeType = getContentTypeResolver().resolve(message.getHeaders());
			if (mimeType == null) {
				if (conversionHint instanceof MimeType hintedMimeType) {
					mimeType = hintedMimeType;
				}
				else {
					return null;
				}
			}

			Schema writerSchema = resolveWriterSchemaForDeserialization(mimeType);
			Schema readerSchema = resolveReaderSchemaForDeserialization(targetClass);

			result = avroSchemaServiceManager().readData(targetClass, payload, readerSchema, writerSchema);
		}
		catch (IOException e) {
			throw new MessageConversionException(message, "Failed to read payload", e);
		}
		return result;
	}

	@Override
	protected Object convertToInternal(Object payload, MessageHeaders headers, Object conversionHint) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			MimeType hintedContentType = null;
			if (conversionHint instanceof MimeType mimeType) {
				hintedContentType = mimeType;
			}
			Schema schema = resolveSchemaForWriting(payload, headers, hintedContentType);
			@SuppressWarnings("unchecked")
			DatumWriter<Object> writer = avroSchemaServiceManager().getDatumWriter(payload.getClass(), schema);
			Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
			writer.write(payload, encoder);
			encoder.flush();
		}
		catch (IOException e) {
			throw new MessageConversionException("Failed to write payload", e);
		}
		return baos.toByteArray();
	}

	protected abstract Schema resolveSchemaForWriting(Object payload, MessageHeaders headers, MimeType hintedContentType);

	protected abstract Schema resolveWriterSchemaForDeserialization(MimeType mimeType);

	protected abstract Schema resolveReaderSchemaForDeserialization(Class<?> targetClass);

}
