/*
 * Copyright 2016-present the original author or authors.
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
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaMessageConverter;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaServiceManager;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaServiceManagerImpl;
import org.springframework.cloud.stream.schema.registry.client.SchemaRegistryClient;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeType;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


/**
 * @author Marius Bogoevici
 */
class AvroSchemaMessageConverterTests {

	static StubSchemaRegistryClient stubSchemaRegistryClient = new StubSchemaRegistryClient();

	@Test
	public void sendMessageWithLocation() throws Exception {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(AvroSourceApplication.class))
				.web(WebApplicationType.NONE).run("--server.port=0",
						"--spring.jmx.enabled=false",
						"--schemaLocation=classpath:schemas/users_v1.schema",
						"--spring.cloud.stream.bindings.myBinding-out-0.contentType=avro/bytes")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);

			User1 user1 = new User1();
			user1.setName("foo" + UUID.randomUUID());
			user1.setFavoriteColor("foo" + UUID.randomUUID());

			streamBridge.send("myBinding-out-0", user1);

			User2 user2 = new User2();
			user2.setFavoriteColor("foo" + UUID.randomUUID().toString());
			user2.setFavoritePlace("foo" + UUID.randomUUID().toString());
			user2.setName("foo" + UUID.randomUUID().toString());

			streamBridge.send("myBinding-out-0", user2);

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive();

			final MessageConverter userMessageConverter = (MessageConverter) context.getBean("userMessageConverter");
			final User1 receivedUser1 = (User1) userMessageConverter.fromMessage(result, User1.class);

			assertThat(receivedUser1).isNotNull();
			assertThat(receivedUser1).isNotSameAs(user1);
			assertThat(receivedUser1.getFavoriteColor()).isEqualTo(user1.getFavoriteColor());
			assertThat(receivedUser1.getName()).isEqualTo(user1.getName());

			result = output.receive();

			final User2 receivedUser2 = (User2) userMessageConverter.fromMessage(result, User2.class);

			assertThat(receivedUser2).isNotNull();
			assertThat(receivedUser2).isNotSameAs(user2);
			assertThat(receivedUser2.getFavoriteColor()).isEqualTo(user2.getFavoriteColor());
			assertThat(receivedUser2.getFavoritePlace()).isEqualTo(user2.getFavoritePlace());
			assertThat(receivedUser2.getName()).isEqualTo(user2.getName());
		}
	}

	@Test
	public void sendMessageWithoutLocation() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(AvroSourceApplication.class))
				.web(WebApplicationType.NONE).run("--server.port=0",
						"--spring.jmx.enabled=false",
						"--spring.cloud.stream.bindings.myBinding-out-0.contentType=avro/bytes")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);

			User1 user1 = new User1();
			user1.setName("foo" + UUID.randomUUID());
			user1.setFavoriteColor("foo" + UUID.randomUUID());

			streamBridge.send("myBinding-out-0", user1);

			User2 user2 = new User2();
			user2.setFavoriteColor("foo" + UUID.randomUUID().toString());
			user2.setFavoritePlace("foo" + UUID.randomUUID().toString());
			user2.setName("foo" + UUID.randomUUID().toString());

			streamBridge.send("myBinding-out-0", user2);

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive();

			final MessageConverter userMessageConverter = (MessageConverter) context.getBean("userMessageConverter");
			final User1 receivedUser1 = (User1) userMessageConverter.fromMessage(result, User1.class);

			assertThat(receivedUser1).isNotNull();
			assertThat(receivedUser1).isNotSameAs(user1);
			assertThat(receivedUser1.getFavoriteColor()).isEqualTo(user1.getFavoriteColor());
			assertThat(receivedUser1.getName()).isEqualTo(user1.getName());

			result = output.receive();

			final User2 receivedUser2 = (User2) userMessageConverter.fromMessage(result, User2.class);

			assertThat(receivedUser2).isNotNull();
			assertThat(receivedUser2).isNotSameAs(user2);
			assertThat(receivedUser2.getFavoriteColor()).isEqualTo(user2.getFavoriteColor());
			assertThat(receivedUser2.getFavoritePlace()).isEqualTo(user2.getFavoritePlace());
			assertThat(receivedUser2.getName()).isEqualTo(user2.getName());
		}
	}

	@EnableAutoConfiguration
	public static class AvroSourceApplication {

		private Resource schemaLocation;

		@Bean
		public SchemaRegistryClient schemaRegistryClient() {
			return stubSchemaRegistryClient;
		}

		@Bean
		public MessageConverter userMessageConverter() throws IOException {
			AvroSchemaServiceManager manager = new AvroSchemaServiceManagerImpl();
			AvroSchemaMessageConverter avroSchemaMessageConverter = new AvroSchemaMessageConverter(
					MimeType.valueOf("avro/bytes"), manager);
			if (this.schemaLocation != null) {
				avroSchemaMessageConverter.setSchemaLocation(this.schemaLocation);
			}
			return avroSchemaMessageConverter;
		}

	}
}
