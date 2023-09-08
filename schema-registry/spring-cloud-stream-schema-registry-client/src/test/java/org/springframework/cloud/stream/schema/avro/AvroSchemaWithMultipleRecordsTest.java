/*
 * Copyright 2020-2022 the original author or authors.
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

import java.io.IOException;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.cloud.stream.schema.registry.avro.AvroMessageConverterProperties;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaMessageConverter;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaRegistryClientMessageConverter;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaServiceManager;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaServiceManagerImpl;
import org.springframework.cloud.stream.schema.registry.avro.DefaultSubjectNamingStrategy;
import org.springframework.cloud.stream.schema.registry.client.SchemaRegistryClient;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Christian Tzolov
 */
public class AvroSchemaWithMultipleRecordsTest {

	@Test
	public void schemaLocationWithMultipleRecords() throws Exception {
		testMultipleRecordsSchemaLoading("avroSchemaRegistryClientMessageConverter",
				"--spring.cloud.stream.schema.avro.schema-locations=classpath:schemas/user1_multiple_records.schema");
	}

	@Test
	public void schemaImportWithMultipleRecords() throws Exception {
		testMultipleRecordsSchemaLoading(
				"avroSchemaRegistryClientMessageConverter",
				"--spring.cloud.stream.schema.avro.schema-imports=classpath:schemas/user1_multiple_records.schema");
	}

	@Test
	public void schemaLocationWithMultipleRecords2() throws Exception {
		testMultipleRecordsSchemaLoading("avroSchemaMessageConverter",
				"--spring.cloud.stream.schema.avro.schema-locations=classpath:schemas/user1_multiple_records.schema");
	}

	private void testMultipleRecordsSchemaLoading(String avroConvertorType, String schemaLoadingProperty)
			throws Exception {
		User1 user1v1 = new org.springframework.cloud.stream.schema.avro.User1();
		user1v1.setFavoriteColor("foo" + UUID.randomUUID());
		user1v1.setName("foo" + UUID.randomUUID());

		org.springframework.cloud.stream.schema.avro.v2.User1 user1v2 = new org.springframework.cloud.stream.schema.avro.v2.User1();
		user1v2.setFavoriteColor("foo" + UUID.randomUUID().toString());
		user1v2.setName("foo" + UUID.randomUUID().toString());
		user1v2.setFavoritePlace("Amsterdam");

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(AvroSourceApplication.class))
						.web(WebApplicationType.NONE).run("--server.port=0",
								"--spring.jmx.enabled=false",
								"--spring.cloud.stream.bindings.myBinding-out-0.contentType=application/*+avro",
								"--avro.message.converter=" + avroConvertorType,
								schemaLoadingProperty)) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);

			streamBridge.send("myBinding-out-0", MessageBuilder.withPayload(user1v1).build());
			streamBridge.send("myBinding-out-0", MessageBuilder.withPayload(user1v2).build());

			final MessageConverter avroSchemaMessageConverter = (MessageConverter) context
					.getBean(avroConvertorType);

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive(); // get first result
			final User1 receivedUser1v1 = (User1) avroSchemaMessageConverter.fromMessage(result, User1.class);
			result = output.receive(); // get second result
			final User1 receivedUser1v2 = (User1) avroSchemaMessageConverter.fromMessage(result, User1.class);

			assertThat(receivedUser1v1.getFavoriteColor()).isEqualTo(user1v1.getFavoriteColor());
			assertThat(receivedUser1v1.getName()).isEqualTo(user1v1.getName());

			assertThat(receivedUser1v2.getFavoriteColor()).isEqualTo(user1v2.getFavoriteColor());
			assertThat(receivedUser1v2.getName()).isEqualTo(user1v2.getName());

		}
	}

	static SchemaRegistryClient stubSchemaRegistryClient = new StubSchemaRegistryClient();

	@EnableConfigurationProperties(AvroMessageConverterProperties.class)
	@EnableAutoConfiguration
	public static class AvroSourceApplication {

		@Bean
		public SchemaRegistryClient schemaRegistryClient() {
			return stubSchemaRegistryClient;
		}

		@Bean
		@ConditionalOnProperty(name = "avro.message.converter", havingValue = "avroSchemaRegistryClientMessageConverter")
		public MessageConverter avroSchemaRegistryClientMessageConverter(AvroMessageConverterProperties properties)
				throws IOException {
			AvroSchemaServiceManager manager = new AvroSchemaServiceManagerImpl();

			AvroSchemaRegistryClientMessageConverter avroSchemaMessageConverter = new AvroSchemaRegistryClientMessageConverter(
					stubSchemaRegistryClient, new ConcurrentMapCacheManager(), manager);
			avroSchemaMessageConverter.setSubjectNamingStrategy(new DefaultSubjectNamingStrategy());

			assertThat(isNotEmpty(properties.getSchemaLocations()) || isNotEmpty(properties.getSchemaImports()))
					.isTrue();

			if (isNotEmpty(properties.getSchemaLocations())) {
				avroSchemaMessageConverter.setSchemaLocations(properties.getSchemaLocations());
			}
			if (isNotEmpty(properties.getSchemaImports())) {
				avroSchemaMessageConverter.setSchemaImports(properties.getSchemaImports());
			}

			return avroSchemaMessageConverter;
		}

		@Bean
		@ConditionalOnProperty(name = "avro.message.converter", havingValue = "avroSchemaMessageConverter")
		public MessageConverter avroSchemaMessageConverter(AvroMessageConverterProperties properties)
				throws IOException {
			AvroSchemaServiceManager manager = new AvroSchemaServiceManagerImpl();

			AvroSchemaMessageConverter avroSchemaMessageConverter = new AvroSchemaMessageConverter(
					MimeType.valueOf("application/avro"), manager);

			assertThat(properties.getSchemaLocations()).isNotEmpty();

			avroSchemaMessageConverter.setSchemaLocation(properties.getSchemaLocations()[0]);

			return avroSchemaMessageConverter;
		}

		private static boolean isNotEmpty(Object[] array) {
			return array != null && array.length > 0;
		}
	}
}
