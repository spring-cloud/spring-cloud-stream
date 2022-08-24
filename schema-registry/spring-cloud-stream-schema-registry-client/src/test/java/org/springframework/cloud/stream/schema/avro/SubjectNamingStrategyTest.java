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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author David Kalosi
 * @author José A. Íñigo
 * @author Christian Tzolov
 */
public class SubjectNamingStrategyTest {

	private static StubSchemaRegistryClient stubSchemaRegistryClient = new StubSchemaRegistryClient();

	@Test
	public void testQualifiedSubjectNamingStrategy() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(AvroSourceApplication.class))
				.web(WebApplicationType.NONE).run("--server.port=0",
						"--spring.jmx.enabled=false",
						"--spring.cloud.stream.bindings.myBinding-out-0.contentType=application/*+avro",
						"--spring.cloud.stream.schema.avro.subjectNamingStrategy="
								+ "org.springframework.cloud.schema.registry.avro.QualifiedSubjectNamingStrategy",
						"--spring.cloud.stream.schema.avro.dynamicSchemaGenerationEnabled=true")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);

			User1 user1 = new User1();
			user1.setFavoriteColor("foo" + UUID.randomUUID());
			user1.setName("foo" + UUID.randomUUID());

			streamBridge.send("myBinding-out-0", user1);

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive();

			final MessageConverter userMessageConverter = (MessageConverter) context.getBean("userMessageConverter");
			final User1 receivedUser1 = (User1) userMessageConverter.fromMessage(result, User1.class);

			assertThat(receivedUser1).isNotNull();
			assertThat(receivedUser1).isNotSameAs(user1);
			assertThat(receivedUser1.getFavoriteColor()).isEqualTo(user1.getFavoriteColor());
			assertThat(receivedUser1.getName()).isEqualTo(user1.getName());

//			assertThat(result.getHeaders().get("contentType")).isEqualTo(MimeType.valueOf(
//					"application/vnd.org.springframework.cloud.schema.avro.User1.v1+avro"));

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
					MimeType.valueOf("application/avro"), manager);
			if (this.schemaLocation != null) {
				avroSchemaMessageConverter.setSchemaLocation(this.schemaLocation);
			}
			return avroSchemaMessageConverter;
		}
	}

}
