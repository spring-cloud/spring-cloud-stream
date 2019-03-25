/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.schema.avro;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import example.avro.Command;
import example.avro.Email;
import example.avro.PushNotification;
import example.avro.Sms;
import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.cache.support.NoOpCacheManager;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.avro.AvroSchemaRegistryClientMessageConverter;
import org.springframework.cloud.stream.schema.avro.DefaultSubjectNamingStrategy;
import org.springframework.cloud.stream.schema.client.DefaultSchemaRegistryClient;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.cloud.stream.schema.server.SchemaRegistryServerApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Vinicius Carvalho
 * @author Sercan Karaoglu
 */
public class AvroMessageConverterSerializationTests {

	Pattern versionedSchema = Pattern.compile(
			"application/" + "vnd" + "\\.([\\p{Alnum}\\$\\.]+)\\.v(\\p{Digit}+)\\+avro");

	Log logger = LogFactory.getLog(getClass());

	private ConfigurableApplicationContext schemaRegistryServerContext;

	public static Command notification() {
		Command messageToSend = getCommandToSend();
		messageToSend.setType("notification");
		PushNotification pushNotification = new PushNotification();
		pushNotification.setArn("google");
		pushNotification.setText("hello");
		messageToSend.setPayload(pushNotification);
		return messageToSend;
	}

	public static Command sms() {
		Command messageToSend = getCommandToSend();
		messageToSend.setType("sms");
		Sms sms = new Sms();
		sms.setPhoneNumber("6141231212");
		sms.setText("hello");
		messageToSend.setPayload(sms);
		return messageToSend;
	}

	public static Command email() {
		Command messageToSend = getCommandToSend();
		messageToSend.setType("email");
		Email email = new Email();
		email.setAddressTo("sercan");
		email.setText("hello");
		email.setTitle("hi");
		messageToSend.setPayload(email);
		return messageToSend;
	}

	public static Command getCommandToSend() {
		Command messageToSend = new Command();
		messageToSend.setCorrelationId("abc");
		return messageToSend;
	}

	@Before
	public void setup() {
		this.schemaRegistryServerContext = SpringApplication.run(
				SchemaRegistryServerApplication.class,
				"--spring.main.allow-bean-definition-overriding=true");
	}

	@After
	public void tearDown() {
		this.schemaRegistryServerContext.close();
	}

	@Test
	public void testSchemaImport() throws Exception {
		SchemaRegistryClient client = new DefaultSchemaRegistryClient();
		AvroSchemaRegistryClientMessageConverter converter = new AvroSchemaRegistryClientMessageConverter(
				client, new NoOpCacheManager());
		converter.setSubjectNamingStrategy(new DefaultSubjectNamingStrategy());
		converter.setDynamicSchemaGenerationEnabled(false);
		converter.setSchemaLocations(this.schemaRegistryServerContext
				.getResources("classpath:schemas/Command.avsc"));
		converter.setSchemaImports(this.schemaRegistryServerContext
				.getResources("classpath:schemas/imports/*.avsc"));
		converter.afterPropertiesSet();
		Command notification = notification();
		Message specificMessage = converter.toMessage(notification,
				new MutableMessageHeaders(Collections.<String, Object>emptyMap()));
		Object o = converter.fromMessage(specificMessage, Command.class);

		assertThat(o).isEqualTo(notification)
				.as("Serialization issue when use schema-imports");
	}

	@Test
	public void sourceWriteSameVersion() throws Exception {
		User specificRecord = new User();
		specificRecord.setName("joe");
		Schema v1 = new Schema.Parser().parse(AvroMessageConverterSerializationTests.class
				.getClassLoader().getResourceAsStream("schemas/user.avsc"));
		GenericRecord genericRecord = new GenericData.Record(v1);
		genericRecord.put("name", "joe");
		SchemaRegistryClient client = new DefaultSchemaRegistryClient();
		AvroSchemaRegistryClientMessageConverter converter = new AvroSchemaRegistryClientMessageConverter(
				client, new NoOpCacheManager());

		converter.setSubjectNamingStrategy(new DefaultSubjectNamingStrategy());
		converter.setDynamicSchemaGenerationEnabled(false);
		converter.afterPropertiesSet();

		Message specificMessage = converter.toMessage(specificRecord,
				new MutableMessageHeaders(Collections.<String, Object>emptyMap()),
				MimeTypeUtils.parseMimeType("application/*+avro"));
		SchemaReference specificRef = extractSchemaReference(MimeTypeUtils.parseMimeType(
				specificMessage.getHeaders().get("contentType").toString()));

		Message genericMessage = converter.toMessage(genericRecord,
				new MutableMessageHeaders(Collections.<String, Object>emptyMap()),
				MimeTypeUtils.parseMimeType("application/*+avro"));
		SchemaReference genericRef = extractSchemaReference(MimeTypeUtils.parseMimeType(
				genericMessage.getHeaders().get("contentType").toString()));

		assertThat(specificRef).isEqualTo(genericRef);
		assertThat(genericRef.getVersion()).isEqualTo(1);
	}

	@Test
	public void testOriginalContentTypeHeaderOnly() throws Exception {
		User specificRecord = new User();
		specificRecord.setName("joe");
		Schema v1 = new Schema.Parser().parse(AvroMessageConverterSerializationTests.class
				.getClassLoader().getResourceAsStream("schemas/user.avsc"));
		GenericRecord genericRecord = new GenericData.Record(v1);
		genericRecord.put("name", "joe");
		SchemaRegistryClient client = new DefaultSchemaRegistryClient();
		client.register("user", "avro", v1.toString());
		AvroSchemaRegistryClientMessageConverter converter = new AvroSchemaRegistryClientMessageConverter(
				client, new NoOpCacheManager());
		converter.setDynamicSchemaGenerationEnabled(false);
		converter.afterPropertiesSet();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DatumWriter<User> writer = new SpecificDatumWriter<>(User.class);
		Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
		writer.write(specificRecord, encoder);
		encoder.flush();
		Message source = MessageBuilder.withPayload(baos.toByteArray())
				.setHeader(MessageHeaders.CONTENT_TYPE,
						MimeTypeUtils.APPLICATION_OCTET_STREAM)
				.setHeader(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE,
						"application/vnd.user.v1+avro")
				.build();
		Object converted = converter.fromMessage(source, User.class);
		assertThat(converted).isNotNull();
		assertThat(specificRecord.getName().toString())
				.isEqualTo(((User) converted).getName().toString());
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
