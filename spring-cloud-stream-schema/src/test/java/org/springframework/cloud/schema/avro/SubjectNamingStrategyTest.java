/*
 * Copyright 2016-2017 the original author or authors.
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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author David Kalosi
 */
public class SubjectNamingStrategyTest {

	static StubSchemaRegistryClient stubSchemaRegistryClient = new StubSchemaRegistryClient();

	@Test
	public void testCustomNamingStrategy() throws Exception {
		ConfigurableApplicationContext sourceContext = SpringApplication.run(
				AvroSourceApplication.class, "--server.port=0", "--debug",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=application/*+avro",
				"--spring.cloud.stream.schema.avro.subjectNamingStrategy="
						+ "org.springframework.cloud.schema.avro.CustomSubjectNamingStrategy",
				"--spring.cloud.stream.schema.avro.dynamicSchemaGenerationEnabled=true");

		Source source = sourceContext.getBean(Source.class);
		User1 user1 = new User1();
		user1.setFavoriteColor("foo" + UUID.randomUUID().toString());
		user1.setName("foo" + UUID.randomUUID().toString());
		source.output().send(MessageBuilder.withPayload(user1).build());

		MessageCollector barSourceMessageCollector = sourceContext
				.getBean(MessageCollector.class);
		Message<?> message = barSourceMessageCollector.forChannel(source.output())
				.poll(1000, TimeUnit.MILLISECONDS);

		assertThat(message.getHeaders().get("contentType")).isEqualTo(MimeType.valueOf(
				"application/vnd.org.springframework.cloud.schema.avro.User1.v1+avro"));
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	public static class AvroSourceApplication {

		@Bean
		public SchemaRegistryClient schemaRegistryClient() {
			return stubSchemaRegistryClient;
		}

	}

}
