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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.schema.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.SchemaRegistrationResponse;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.ObjectUtils;

/**
 * A {@link org.springframework.messaging.converter.MessageConverter} for Apache Avro
 * schemas, with the ability to publish and retrieve schemas stored in a schema server.
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 */
public class AvroSchemaRegistryClientMessageConverter extends AbstractAvroMessageConverter
		implements ApplicationContextAware,
		InitializingBean {

	public static final String AVRO_FORMAT = "avro";

	private static Pattern VERSIONED_SCHEMA = Pattern.compile(
			"application/vnd\\.([\\p{Alnum}\\$\\.]+)\\.v(\\p{Digit}+)\\+avro");

	private ApplicationContext applicationContext;

	private boolean dynamicSchemaGenerationEnabled;

	private Map<String, Schema> localSchemaMap = new HashMap<>();

	private Schema readerSchema;

	private Resource[] schemaLocations;

	private SchemaRegistryClient schemaRegistryClient;

	public AvroSchemaRegistryClientMessageConverter(SchemaRegistryClient schemaRegistryClient) {
		super(Arrays.asList(new MimeType("application", "avro"), new MimeType("application", "*+avro")));
		Assert.notNull(schemaRegistryClient, "cannot be null");
		this.schemaRegistryClient = schemaRegistryClient;
	}

	public void setDynamicSchemaGenerationEnabled(boolean dynamicSchemaGenerationEnabled) {
		this.dynamicSchemaGenerationEnabled = dynamicSchemaGenerationEnabled;
	}

	public boolean isDynamicSchemaGenerationEnabled() {
		return this.dynamicSchemaGenerationEnabled;
	}

	public void setSchemaLocations(Resource[] schemaLocations) {
		Assert.notEmpty(schemaLocations, "cannot be empty");
		this.schemaLocations = schemaLocations;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
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
		return schema.getFullName().toLowerCase();
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
		return (MimeType.valueOf("application/avro").includes(mimeType)
				|| MimeType.valueOf("application/*+avro").includes(mimeType));
	}

	@Override
	protected Schema resolveWriterSchema(Object payload, MessageHeaders headers,
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
			String schemaContents = this.schemaRegistryClient.fetch(schemaReference);
			schema = parser.parse(schemaContents);
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
		Matcher schemaMatcher = VERSIONED_SCHEMA.matcher(mimeType.toString());
		if (schemaMatcher.find()) {
			String subject = schemaMatcher.group(1);
			Integer version = Integer.parseInt(schemaMatcher.group(2));
			schemaReference = new SchemaReference(subject, version, AVRO_FORMAT);
		}
		return schemaReference;
	}

	@Override
	protected Schema resolveReaderSchema(MimeType mimeType) {
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
