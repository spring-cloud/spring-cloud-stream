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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectData;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.schema.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.SchemaRegistrationResponse;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.core.io.Resource;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.ObjectUtils;

/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * for Apache Avro, with the ability to publish and retrieve schemas
 * stored in a schema server, allowing for schema evolution in applications.
 * The supported content types are in the form `application/*+avro`.
 *
 * During the conversion to a message, the converter will set the 'contentType'
 * header to 'application/[prefix].[subject].v[version]+avro', where:
 *
 * <li>
 * <ul><i>prefix</i> is a configurable prefix (default 'vnd');</ul>
 * <ul><i>subject</i> is a subject derived from the type of the outgoing object - typically the class name;</ul>
 * <ul><i>version</i> is the schema version for the given subject;</ul>
 * </li>
 *
 * When converting from a message, the converter will parse the content-type
 * and use it to fetch and cache the writer schema using the provided
 * {@link SchemaRegistryClient}.
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 */
public class AvroSchemaRegistryClientMessageConverter extends AbstractAvroMessageConverter implements InitializingBean {

	public static final String AVRO_FORMAT = "avro";

	public static final Pattern PREFIX_VALIDATION_PATTERN = Pattern.compile("[\\p{Alnum}]");

	private Pattern versionedSchema;

	private boolean dynamicSchemaGenerationEnabled;

	private Map<String, Schema> localSchemaMap = new HashMap<>();

	private Schema readerSchema;

	private Resource[] schemaLocations;

	private SchemaRegistryClient schemaRegistryClient;

	private String prefix = "vnd";

	/**
	 * Creates a new instance, configuring it with a {@link SchemaRegistryClient}.
	 * @param schemaRegistryClient the {@link SchemaRegistryClient} used to interact with the schema registry server.
	 */
	public AvroSchemaRegistryClientMessageConverter(SchemaRegistryClient schemaRegistryClient) {
		super(Arrays.asList(new MimeType("application", "*+avro")));
		Assert.notNull(schemaRegistryClient, "cannot be null");
		this.schemaRegistryClient = schemaRegistryClient;
	}

	/**
	 * Allows the converter to generate and register schemas automatically.
	 * If set to false, it only allows the converter to use pre-registered schemas.
	 * Default 'true'.
	 * @param dynamicSchemaGenerationEnabled true if dynamic schema generation is enabled
	 */
	public void setDynamicSchemaGenerationEnabled(boolean dynamicSchemaGenerationEnabled) {
		this.dynamicSchemaGenerationEnabled = dynamicSchemaGenerationEnabled;
	}

	public boolean isDynamicSchemaGenerationEnabled() {
		return this.dynamicSchemaGenerationEnabled;
	}

	/**
	 * A set of locations where the converter can load schemas from.
	 * Schemas provided at these locations will be registered automatically.
	 *
	 * @param schemaLocations
	 */
	public void setSchemaLocations(Resource[] schemaLocations) {
		Assert.notEmpty(schemaLocations, "cannot be empty");
		this.schemaLocations = schemaLocations;
	}

	/**
	 * Set the prefix to be used in the publised subtype. Default 'vnd'.
	 * @param prefix
	 */
	public void setPrefix(String prefix) {
		Assert.hasText(prefix, "Prefix cannot be empty");
		Assert.isTrue(!PREFIX_VALIDATION_PATTERN.matcher(this.prefix).matches(), "Invalid prefix:" + this.prefix);
		this.prefix = prefix;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.versionedSchema = Pattern.compile(
				"application/" + this.prefix + "\\.([\\p{Alnum}\\$\\.]+)\\.v(\\p{Digit}+)\\+avro");
		if (!ObjectUtils.isEmpty(this.schemaLocations)) {
			this.logger.info("Scanning avro schema resources on classpath");
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Parsing" + this.schemaLocations.length);
			}
			for (Resource schemaLocation : this.schemaLocations) {
				try {
					Schema schema = parseSchema(schemaLocation);
					if (this.logger.isInfoEnabled()) {
						this.logger.info("Resource " + schemaLocation.getFilename() + " parsed into schema " + schema
								.getNamespace() + "." + schema.getName());
					}
					this.schemaRegistryClient.register(toSubject(schema), AVRO_FORMAT, schema.toString(true));
					if (this.logger.isInfoEnabled()) {
						this.logger.info("Schema " + schema.getName() + " registered with id " + schema);
					}
					this.localSchemaMap.put(schema.getNamespace() + "." + schema.getName(), schema);
				}
				catch (IOException e) {
					if (this.logger.isWarnEnabled()) {
						this.logger.warn("Failed to parse schema at " + schemaLocation.getFilename(), e);
					}
				}
			}
		}
	}

	protected String toSubject(Schema schema) {
		return schema.getName().toLowerCase();
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		// we support all types
		return true;
	}

	@Override
	protected boolean supportsMimeType(MessageHeaders headers) {
		if (super.supportsMimeType(headers)) {
			return true;
		}
		MimeType mimeType = getContentTypeResolver().resolve(headers);
		return MimeType.valueOf("application/*+avro").includes(mimeType);
	}

	@Override
	protected Schema resolveSchemaForWriting(Object payload, MessageHeaders headers,
			MimeType hintedContentType) {
		Schema schema;
		SchemaReference schemaReference = extractSchemaReference(hintedContentType);
		// the mimeType does not contain a schema reference
		if (schemaReference == null) {
			schema = extractSchemaForWriting(payload);
			SchemaRegistrationResponse schemaRegistrationResponse = this.schemaRegistryClient.register(
					toSubject(schema), AVRO_FORMAT, schema.toString(true));
			schemaReference = schemaRegistrationResponse.getSchemaReference();
		}
		else {
			Schema.Parser parser = new Schema.Parser();
			try {
				String schemaContents = this.schemaRegistryClient.fetch(schemaReference);
				schema = parser.parse(schemaContents);
			} catch (SchemaNotFoundException e) {
				schema = extractSchemaForWriting(payload);
				if (schema != null) {
					SchemaRegistrationResponse schemaRegistrationResponse = this.schemaRegistryClient.register(
							toSubject(schema), AVRO_FORMAT, schema.toString(true));
				}
				else {
					throw e;
				}
			}
		}
		if (headers instanceof MutableMessageHeaders) {
			headers.put(MessageHeaders.CONTENT_TYPE,
					"application/vnd." + schemaReference.getSubject() + ".v" + schemaReference
							.getVersion() + "+avro");
		}
		return schema;
	}

	private SchemaReference extractSchemaReference(MimeType mimeType) {
		SchemaReference schemaReference = null;
		Matcher schemaMatcher = this.versionedSchema.matcher(mimeType.toString());
		if (schemaMatcher.find()) {
			String subject = schemaMatcher.group(1);
			Integer version = Integer.parseInt(schemaMatcher.group(2));
			schemaReference = new SchemaReference(subject, version, AVRO_FORMAT);
		}
		return schemaReference;
	}

	@Override
	protected Schema resolveWriterSchemaForDeserialization(MimeType mimeType) {
		if (this.readerSchema == null) {
			Schema schema = null;
			SchemaReference schemaReference = extractSchemaReference(mimeType);
			if (schemaReference != null) {
				String schemaContent = this.schemaRegistryClient.fetch(schemaReference);
				schema = new Schema.Parser().parse(schemaContent);
			}
			return schema;
		}
		else {
			return this.readerSchema;
		}
	}

	@Override
	protected Schema resolveReaderSchemaForDeserialization(Class<?> targetClass) {
		return this.readerSchema;
	}

	public void setReaderSchema(Resource readerSchema) {
		Assert.notNull(readerSchema, "cannot be null");
		try {
			this.readerSchema = parseSchema(readerSchema);
		}
		catch (IOException e) {
			throw new BeanInitializationException("Cannot initialize reader schema", e);
		}
	}

	private Schema extractSchemaForWriting(Object payload) {
		Schema schema = null;
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Obtaining schema for class " + payload.getClass());
		}
		if (GenericContainer.class.isAssignableFrom(payload.getClass())) {
			schema = ((GenericContainer) payload).getSchema();
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Avro type detected, using schema from object");
			}
		}
		else {
			schema = this.localSchemaMap.get(payload.getClass().getName());
			if (schema == null) {
				if (!isDynamicSchemaGenerationEnabled()) {
					throw new SchemaNotFoundException(
							String.format("No schema found in the local cache for %s, and dynamic schema generation " +
									"is not enabled", payload.getClass()));
				}
				else {
					schema = ReflectData.get().getSchema(payload.getClass());
					this.schemaRegistryClient.register(toSubject(schema), AVRO_FORMAT, schema.toString(true));
				}
				this.localSchemaMap.put(payload.getClass().getName(), schema);
			}
		}
		return schema;
	}
}
