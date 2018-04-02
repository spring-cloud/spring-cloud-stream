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

import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectData;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.NoOpCacheManager;
import org.springframework.cloud.stream.schema.ParsedSchema;
import org.springframework.cloud.stream.schema.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.SchemaRegistrationResponse;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.core.io.Resource;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.ObjectUtils;

/**
 * A {@link org.springframework.messaging.converter.MessageConverter} for Apache Avro,
 * with the ability to publish and retrieve schemas stored in a schema server, allowing
 * for schema evolution in applications. The supported content types are in the form
 * `application/*+avro`.
 *
 * During the conversion to a message, the converter will set the 'contentType' header to
 * 'application/[prefix].[subject].v[version]+avro', where:
 *
 * <li>
 * <ul>
 * <i>prefix</i> is a configurable prefix (default 'vnd');
 * </ul>
 * <ul>
 * <i>subject</i> is a subject derived from the type of the outgoing object - typically
 * the class name;
 * </ul>
 * <ul>
 * <i>version</i> is the schema version for the given subject;
 * </ul>
 * </li>
 *
 * When converting from a message, the converter will parse the content-type and use it to
 * fetch and cache the writer schema using the provided {@link SchemaRegistryClient}.
 *
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
public class AvroSchemaRegistryClientMessageConverter extends AbstractAvroMessageConverter
		implements InitializingBean {

	public static final String AVRO_FORMAT = "avro";

	public static final Pattern PREFIX_VALIDATION_PATTERN = Pattern
			.compile("[\\p{Alnum}]");

	public static final String CACHE_PREFIX = "org.springframework.cloud.stream.schema";

	public static final String REFLECTION_CACHE_NAME = CACHE_PREFIX + ".reflectionCache";

	public static final String SCHEMA_CACHE_NAME = CACHE_PREFIX + ".schemaCache";

	public static final String REFERENCE_CACHE_NAME = CACHE_PREFIX + ".referenceCache";

	public static final MimeType DEFAULT_AVRO_MIME_TYPE = new MimeType("application", "*+" + AVRO_FORMAT);

	private Pattern versionedSchema;

	private boolean dynamicSchemaGenerationEnabled;

	private CacheManager cacheManager;

	private Schema readerSchema;

	private Resource[] schemaLocations;

	private SchemaRegistryClient schemaRegistryClient;

	private String prefix = "vnd";

	private SubjectNamingStrategy subjectNamingStrategy;

	/**
	 * Creates a new instance, configuring it with {@link SchemaRegistryClient} and
	 * {@link CacheManager}.
	 * @param schemaRegistryClient the {@link SchemaRegistryClient} used to interact with
	 * the schema registry server.
	 * @param cacheManager instance of {@link CacheManager} to cache parsed schemas. If
	 * caching is not required use {@link NoOpCacheManager}
	 */
	public AvroSchemaRegistryClientMessageConverter(SchemaRegistryClient schemaRegistryClient, CacheManager cacheManager) {
		super(Collections.singletonList(DEFAULT_AVRO_MIME_TYPE));
		Assert.notNull(schemaRegistryClient, "cannot be null");
		Assert.notNull(cacheManager, "'cacheManager' cannot be null");
		this.schemaRegistryClient = schemaRegistryClient;
		this.cacheManager = cacheManager;
	}

	public boolean isDynamicSchemaGenerationEnabled() {
		return this.dynamicSchemaGenerationEnabled;
	}

	/**
	 * Allows the converter to generate and register schemas automatically. If set to
	 * false, it only allows the converter to use pre-registered schemas. Default 'true'.
	 * @param dynamicSchemaGenerationEnabled true if dynamic schema generation is enabled
	 */
	public void setDynamicSchemaGenerationEnabled(
			boolean dynamicSchemaGenerationEnabled) {
		this.dynamicSchemaGenerationEnabled = dynamicSchemaGenerationEnabled;
	}

	/**
	 * A set of locations where the converter can load schemas from. Schemas provided at
	 * these locations will be registered automatically.
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
		Assert.isTrue(!PREFIX_VALIDATION_PATTERN.matcher(this.prefix).matches(),
				"Invalid prefix:" + this.prefix);
		this.prefix = prefix;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.versionedSchema = Pattern.compile("application/" + this.prefix
				+ "\\.([\\p{Alnum}\\$\\.]+)\\.v(\\p{Digit}+)\\+"+AVRO_FORMAT);
		if (!ObjectUtils.isEmpty(this.schemaLocations)) {
			this.logger.info("Scanning avro schema resources on classpath");
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Parsing" + this.schemaLocations.length);
			}
			for (Resource schemaLocation : this.schemaLocations) {
				try {
					Schema schema = parseSchema(schemaLocation);
					if (this.logger.isInfoEnabled()) {
						this.logger.info("Resource " + schemaLocation.getFilename()
								+ " parsed into schema " + schema.getNamespace() + "."
								+ schema.getName());
					}
					this.schemaRegistryClient.register(toSubject(schema), AVRO_FORMAT,
							schema.toString());
					if (this.logger.isInfoEnabled()) {
						this.logger.info("Schema " + schema.getName()
								+ " registered with id " + schema);
					}
					this.cacheManager.getCache(REFLECTION_CACHE_NAME)
							.put(schema.getNamespace() + "." + schema.getName(), schema);
				}
				catch (IOException e) {
					if (this.logger.isWarnEnabled()) {
						this.logger.warn("Failed to parse schema at "
								+ schemaLocation.getFilename(), e);
					}
				}
			}
		}
		if (this.cacheManager instanceof NoOpCacheManager) {
			logger.warn("Schema caching is effectively disabled "
					+ "since configured cache manager is a NoOpCacheManager. If this was not "
					+ "the intention, please provide the appropriate instance of CacheManager "
					+ "(i.e., ConcurrentMapCacheManager).");
		}
	}

	protected String toSubject(Schema schema) {
		return subjectNamingStrategy.toSubject(schema);
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
		return DEFAULT_AVRO_MIME_TYPE.includes(mimeType);
	}

	@Override
	protected Schema resolveSchemaForWriting(Object payload, MessageHeaders headers,
			MimeType hintedContentType) {

		Schema schema;
		schema = extractSchemaForWriting(payload);
		ParsedSchema parsedSchema = this.cacheManager.getCache(REFERENCE_CACHE_NAME)
				.get(schema, ParsedSchema.class);

		if (parsedSchema == null) {
			parsedSchema = new ParsedSchema(schema);
			this.cacheManager.getCache(REFERENCE_CACHE_NAME).putIfAbsent(schema,
					parsedSchema);
		}

		if (parsedSchema.getRegistration() == null) {
			SchemaRegistrationResponse response = this.schemaRegistryClient.register(
					toSubject(schema), AVRO_FORMAT, parsedSchema.getRepresentation());
			parsedSchema.setRegistration(response);

		}

		SchemaReference schemaReference = parsedSchema.getRegistration()
				.getSchemaReference();

		DirectFieldAccessor dfa = new DirectFieldAccessor(headers);
		@SuppressWarnings("unchecked")
		Map<String, Object> _headers = (Map<String, Object>) dfa.getPropertyValue("headers");
		_headers.put(MessageHeaders.CONTENT_TYPE,
				"application/" + this.prefix + "." + schemaReference.getSubject()
				+ ".v" + schemaReference.getVersion() + "+" + AVRO_FORMAT);

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
			SchemaReference schemaReference = extractSchemaReference(mimeType);
			if (schemaReference != null) {
				ParsedSchema parsedSchema = cacheManager.getCache(REFERENCE_CACHE_NAME).get(schemaReference, ParsedSchema.class);
				if (parsedSchema == null) {
					String schemaContent = this.schemaRegistryClient.fetch(schemaReference);
					if (schemaContent != null) {
						Schema schema = new Schema.Parser().parse(schemaContent);
						parsedSchema = new ParsedSchema(schema);
						cacheManager.getCache(REFERENCE_CACHE_NAME).putIfAbsent(schemaReference, parsedSchema);
					}
				}
				if (parsedSchema != null) {
					return parsedSchema.getSchema();
				}
			}
		}
		return this.readerSchema;
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
			schema = this.cacheManager.getCache(REFLECTION_CACHE_NAME)
					.get(payload.getClass().getName(), Schema.class);
			if (schema == null) {
				if (!isDynamicSchemaGenerationEnabled()) {
					throw new SchemaNotFoundException(String
							.format("No schema found in the local cache for %s, and dynamic schema generation "
									+ "is not enabled", payload.getClass()));
				}
				else {
					schema = ReflectData.get().getSchema(payload.getClass());
				}
				this.cacheManager.getCache(REFLECTION_CACHE_NAME)
						.put(payload.getClass().getName(), schema);
			}
		}
		return schema;
	}

	/**
	 * @deprecated as of release 1.0.4. Please use the constructor to inject CacheManager
	 */
	@Deprecated
	public void setCacheManager(CacheManager cacheManager) {
		Assert.notNull(cacheManager, "'cacheManager' cannot be null");
		this.cacheManager = cacheManager;
	}

	public void setSubjectNamingStrategy(SubjectNamingStrategy subjectNamingStrategy) {
		this.subjectNamingStrategy = subjectNamingStrategy;
	}
}
