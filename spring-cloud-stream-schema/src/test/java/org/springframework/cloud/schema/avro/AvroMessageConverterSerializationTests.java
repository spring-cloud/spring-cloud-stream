/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.schema.avro;

import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.cloud.stream.binder.StringConvertingContentTypeResolver;
import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.avro.AvroSchemaRegistryClientMessageConverter;
import org.springframework.cloud.stream.schema.client.DefaultSchemaRegistryClient;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.cloud.stream.schema.server.SchemaRegistryServerApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.Message;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * @author Vinicius Carvalho
 */
public class AvroMessageConverterSerializationTests {

	Pattern versionedSchema = Pattern.compile(
			"application/" + "vnd" + "\\.([\\p{Alnum}\\$\\.]+)\\.v(\\p{Digit}+)\\+avro");

	Log logger = LogFactory.getLog(getClass());

	private ConfigurableApplicationContext schemaRegistryServerContext;

	@Before
	public void setup() {
		schemaRegistryServerContext = SpringApplication.run(
				SchemaRegistryServerApplication.class);
	}

	@After
	public void tearDown() {
		schemaRegistryServerContext.close();
	}

	@Test
	public void sourceWriteSameVersion() throws Exception {
		User specificRecord = new User();
		specificRecord.setName("joe");
		Schema v1 = new Schema.Parser().parse(
				AvroMessageConverterSerializationTests.class.getClassLoader().getResourceAsStream("schemas/user.avsc"));
		GenericRecord genericRecord = new GenericData.Record(v1);
		genericRecord.put("name", "joe");
		SchemaRegistryClient client = new DefaultSchemaRegistryClient();
		AvroSchemaRegistryClientMessageConverter converter = new AvroSchemaRegistryClientMessageConverter(client);
		converter.setDynamicSchemaGenerationEnabled(false);
		converter.setContentTypeResolver(new StringConvertingContentTypeResolver());
		converter.setCacheManager(new ConcurrentMapCacheManager());
		converter.afterPropertiesSet();

		Message specificMessage = converter.toMessage(specificRecord,
				new MutableMessageHeaders(Collections.<String, Object>emptyMap()),
				MimeTypeUtils.parseMimeType("application/*+avro"));
		SchemaReference specificRef = extractSchemaReference(
				MimeTypeUtils.parseMimeType(specificMessage.getHeaders().get("contentType").toString()));

		Message genericMessage = converter.toMessage(genericRecord,
				new MutableMessageHeaders(Collections.<String, Object>emptyMap()),
				MimeTypeUtils.parseMimeType("application/*+avro"));
		SchemaReference genericRef = extractSchemaReference(
				MimeTypeUtils.parseMimeType(genericMessage.getHeaders().get("contentType").toString()));

		Assert.assertEquals(genericRef, specificRef);
		Assert.assertEquals(1, genericRef.getVersion());
	}

	private SchemaReference extractSchemaReference(MimeType mimeType) {
		SchemaReference schemaReference = null;
		Matcher schemaMatcher = this.versionedSchema.matcher(mimeType.toString());
		if (schemaMatcher.find()) {
			String subject = schemaMatcher.group(1);
			Integer version = Integer.parseInt(schemaMatcher.group(2));
			schemaReference = new SchemaReference(subject, version,
					AvroSchemaRegistryClientMessageConverter.AVRO_FORMAT);
		}
		return schemaReference;
	}

}
