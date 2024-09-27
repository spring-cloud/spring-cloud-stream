/*
 * Copyright 2016-2019 the original author or authors.
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
import java.util.Collection;

import org.apache.avro.Schema;

import org.springframework.core.io.Resource;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

/**
 * A {@link org.springframework.messaging.converter.MessageConverter} using Apache Avro.
 * The schema for serializing and deserializing will be automatically inferred from the
 * class for {@link org.apache.avro.specific.SpecificRecord} and regular classes, unless a
 * specific schema is set, case in which that schema will be used instead. For converting
 * to {@link org.apache.avro.generic.GenericRecord} targets, a schema must be set.s
 *
 * @author Marius Bogoevici
 * @author Ish Mahajan
 */

public class AvroSchemaMessageConverter extends AbstractAvroMessageConverter {

	private Schema schema;

	/**
	 * Create a {@link AvroSchemaMessageConverter}. Uses the default {@link MimeType} of
	 * {@code "application/avro"}.
	 * @param manager for schema management
	 */
	public AvroSchemaMessageConverter(AvroSchemaServiceManager manager) {
		super(new MimeType("application", "avro"), manager);
	}

	/**
	 * Create a {@link AvroSchemaMessageConverter}. The converter will be used for the
	 * provided {@link MimeType}.
	 * @param supportedMimeType mime type to be supported by
	 * {@link AvroSchemaMessageConverter}
	 * @param manager for schema management
	 */
	public AvroSchemaMessageConverter(MimeType supportedMimeType, AvroSchemaServiceManager manager) {
		super(supportedMimeType, manager);
	}

	/**
	 * Create a {@link AvroSchemaMessageConverter}. The converter will be used for the
	 * provided {@link MimeType}s.
	 * @param supportedMimeTypes the mime types supported by this converter
	 * @param manager for schema management
	 */
	public AvroSchemaMessageConverter(Collection<MimeType> supportedMimeTypes, AvroSchemaServiceManager manager) {
		super(supportedMimeTypes, manager);
	}

	public Schema getSchema() {
		return this.schema;
	}

	/**
	 * Sets the Apache Avro schema to be used by this converter.
	 * @param schema schema to be used by this converter
	 */
	public void setSchema(Schema schema) {
		Assert.notNull(schema, "schema cannot be null");
		this.schema = schema;
	}

	/**
	 * The location of the Apache Avro schema to be used by this converter.
	 * @param schemaLocation the location of the schema used by this converter.
	 */
	public void setSchemaLocation(Resource schemaLocation) {
		Assert.notNull(schemaLocation, "schema cannot be null");
		try {
			this.schema = parseSchema(schemaLocation);
		}
		catch (IOException e) {
			throw new IllegalStateException("Schema cannot be parsed:", e);
		}
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return true;
	}

	@Override
	protected Schema resolveWriterSchemaForDeserialization(MimeType mimeType) {
		return this.schema;
	}

	@Override
	protected Schema resolveReaderSchemaForDeserialization(Class<?> targetClass) {
		return this.schema;
	}

	@Override
	protected Schema resolveSchemaForWriting(Object payload, MessageHeaders headers,
			MimeType hintedContentType) {
		return this.schema;
	}

}
