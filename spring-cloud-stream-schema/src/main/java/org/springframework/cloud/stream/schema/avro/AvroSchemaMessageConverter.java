/*
 * Copyright 2016 the original author or authors.
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
import java.util.Collection;

import org.apache.avro.Schema;

import org.springframework.core.io.Resource;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeType;

/**
 * A
 * @author Marius Bogoevici
 */

public class AvroSchemaMessageConverter extends AbstractAvroMessageConverter {

	private final Schema readerSchema;

	/**
	 * Create a {@link AvroSchemaMessageConverter} using the provided {@link Schema}.
	 * Uses the default {@link MimeType} of {@code "application/avro"}.
	 * @param schema the Avro schema used by this converter
	 */
	public AvroSchemaMessageConverter(Schema schema) {
		super(new MimeType("application", "avro"));
		this.readerSchema = schema;
	}

	/**
	 * Create a {@link AvroSchemaMessageConverter} using the schema contained in the {@link Resource}.
	 * Uses the default {@link MimeType} of {@code "application/avro"}.
	 * @param schemaLocation the location of the schema
	 * @throws IOException if the resource cannot be found
	 */
	public AvroSchemaMessageConverter(Resource schemaLocation) throws IOException {
		this(parseSchema(schemaLocation));
	}

	/**
	 * Create a {@link AvroSchemaMessageConverter} using the provided {@link Schema}.
	 * The converter will be used for the provided {@link MimeType}.
	 * @param supportedMimeType the mime type supported by this converter
	 * @param schema            the Avro schema used by this converter
	 */
	public AvroSchemaMessageConverter(MimeType supportedMimeType, Schema schema) {
		super(supportedMimeType);
		this.readerSchema = schema;
	}

	/**
	 * Create a {@link AvroSchemaMessageConverter} using the schema contained in the {@link Resource}.
	 * The converter will be used for the provided {@link MimeType}.
	 * @param supportedMimeType the mime type supported by this converter
	 * @param schemaLocation    the location of the schema
	 * @throws IOException if the resource cannot be found
	 */
	public AvroSchemaMessageConverter(MimeType supportedMimeType, Resource schemaLocation) throws IOException {
		this(supportedMimeType, parseSchema(schemaLocation));
	}

	/**
	 * Create a {@link AvroSchemaMessageConverter} using the provided {@link Schema}.
	 * The converter will be used for the provided {@link MimeType}s.
	 * @param supportedMimeTypes the mime types supported by this converter
	 * @param schema             the Avro schema used by this converter
	 */
	public AvroSchemaMessageConverter(Collection<MimeType> supportedMimeTypes, Schema schema) {
		super(supportedMimeTypes);
		this.readerSchema = schema;
	}

	/**
	 * Create a {@link AvroSchemaMessageConverter} using the {@link Schema} contained in the {@link Resource}.
	 * The converter will be used for the provided {@link MimeType}s.
	 * @param supportedMimeTypes the mime types supported by this converter
	 * @param schemaLocation     the Avro schema used by this converter
	 */
	public AvroSchemaMessageConverter(Collection<MimeType> supportedMimeTypes, Resource schemaLocation)
			throws IOException {
		this(supportedMimeTypes, parseSchema(schemaLocation));
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return true;
	}

	@Override
	protected Schema resolveReaderSchema(MimeType mimeType) {
		return this.readerSchema;
	}

	@Override
	protected Schema resolveWriterSchema(Object payload, MessageHeaders headers,
			MimeType hintedContentType) {
		return this.readerSchema;
	}
}
