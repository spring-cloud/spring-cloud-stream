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

package org.springframework.cloud.schema.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.schema.avro.AvroSchemaMessageConverter;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 */
public class AvroSchemaMessageConverterTests {

	static StubSchemaRegistryClient stubSchemaRegistryClient = new StubSchemaRegistryClient();

	@Test
	public void testSendMessageWithLocation() throws Exception {
		ConfigurableApplicationContext sourceContext = SpringApplication.run(
				AvroSourceApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--schemaLocation=classpath:schemas/users_v1.schema",
				"--spring.cloud.stream.schemaRegistryClient.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=avro/bytes");
		Source source = sourceContext.getBean(Source.class);
		User1 firstOutboundFoo = new User1();
		firstOutboundFoo.setName("foo" + UUID.randomUUID().toString());
		firstOutboundFoo.setFavoriteColor("foo" + UUID.randomUUID().toString());
		source.output().send(MessageBuilder.withPayload(firstOutboundFoo).build());
		MessageCollector sourceMessageCollector = sourceContext
				.getBean(MessageCollector.class);
		Message<?> outboundMessage = sourceMessageCollector.forChannel(source.output())
				.poll(1000, TimeUnit.MILLISECONDS);

		ConfigurableApplicationContext barSourceContext = SpringApplication.run(
				AvroSourceApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--schemaLocation=classpath:schemas/users_v1.schema",
				"--spring.cloud.stream.schemaRegistryClient.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=avro/bytes");
		Source barSource = barSourceContext.getBean(Source.class);
		User2 firstOutboundUser2 = new User2();
		firstOutboundUser2.setFavoriteColor("foo" + UUID.randomUUID().toString());
		firstOutboundUser2.setFavoritePlace("foo" + UUID.randomUUID().toString());
		firstOutboundUser2.setName("foo" + UUID.randomUUID().toString());
		barSource.output().send(MessageBuilder.withPayload(firstOutboundUser2).build());
		MessageCollector barSourceMessageCollector = barSourceContext
				.getBean(MessageCollector.class);
		Message<?> barOutboundMessage = barSourceMessageCollector
				.forChannel(barSource.output()).poll(1000, TimeUnit.MILLISECONDS);

		assertThat(barOutboundMessage).isNotNull();

		User2 secondUser2OutboundPojo = new User2();
		secondUser2OutboundPojo.setFavoriteColor("foo" + UUID.randomUUID().toString());
		secondUser2OutboundPojo.setFavoritePlace("foo" + UUID.randomUUID().toString());
		secondUser2OutboundPojo.setName("foo" + UUID.randomUUID().toString());
		source.output().send(MessageBuilder.withPayload(secondUser2OutboundPojo).build());
		Message<?> secondBarOutboundMessage = sourceMessageCollector
				.forChannel(source.output()).poll(1000, TimeUnit.MILLISECONDS);

		ConfigurableApplicationContext sinkContext = SpringApplication.run(
				AvroSinkApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.schemaRegistryClient.enabled=false",
				"--schemaLocation=classpath:schemas/users_v1.schema");
		Sink sink = sinkContext.getBean(Sink.class);
		sink.input().send(outboundMessage);
		sink.input().send(barOutboundMessage);
		sink.input().send(secondBarOutboundMessage);
		List<User1> receivedUsers = sinkContext
				.getBean(AvroSinkApplication.class).receivedUsers;
		assertThat(receivedUsers).hasSize(3);
		assertThat(receivedUsers.get(0)).isNotSameAs(firstOutboundFoo);
		assertThat(receivedUsers.get(0).getFavoriteColor())
				.isEqualTo(firstOutboundFoo.getFavoriteColor());
		assertThat(receivedUsers.get(0).getName()).isEqualTo(firstOutboundFoo.getName());

		assertThat(receivedUsers.get(1)).isNotSameAs(firstOutboundUser2);
		assertThat(receivedUsers.get(1).getFavoriteColor())
				.isEqualTo(firstOutboundUser2.getFavoriteColor());
		assertThat(receivedUsers.get(1).getName())
				.isEqualTo(firstOutboundUser2.getName());

		assertThat(receivedUsers.get(2)).isNotSameAs(secondUser2OutboundPojo);
		assertThat(receivedUsers.get(2).getFavoriteColor())
				.isEqualTo(secondUser2OutboundPojo.getFavoriteColor());
		assertThat(receivedUsers.get(2).getName())
				.isEqualTo(secondUser2OutboundPojo.getName());

		sourceContext.close();
	}

	@Test
	public void testSendMessageWithoutLocation() throws Exception {
		ConfigurableApplicationContext sourceContext = SpringApplication.run(
				AvroSourceApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.schemaRegistryClient.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=avro/bytes");
		Source source = sourceContext.getBean(Source.class);
		User1 firstOutboundFoo = new User1();
		firstOutboundFoo.setName("foo" + UUID.randomUUID().toString());
		firstOutboundFoo.setFavoriteColor("foo" + UUID.randomUUID().toString());
		source.output().send(MessageBuilder.withPayload(firstOutboundFoo).build());
		MessageCollector sourceMessageCollector = sourceContext
				.getBean(MessageCollector.class);
		Message<?> outboundMessage = sourceMessageCollector.forChannel(source.output())
				.poll(1000, TimeUnit.MILLISECONDS);

		ConfigurableApplicationContext barSourceContext = SpringApplication.run(
				AvroSourceApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.schemaRegistryClient.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=avro/bytes");
		Source barSource = barSourceContext.getBean(Source.class);
		User2 firstOutboundUser2 = new User2();
		firstOutboundUser2.setFavoriteColor("foo" + UUID.randomUUID().toString());
		firstOutboundUser2.setFavoritePlace("foo" + UUID.randomUUID().toString());
		firstOutboundUser2.setName("foo" + UUID.randomUUID().toString());
		barSource.output().send(MessageBuilder.withPayload(firstOutboundUser2).build());
		MessageCollector barSourceMessageCollector = barSourceContext
				.getBean(MessageCollector.class);
		Message<?> barOutboundMessage = barSourceMessageCollector
				.forChannel(barSource.output()).poll(1000, TimeUnit.MILLISECONDS);

		assertThat(barOutboundMessage).isNotNull();

		User2 secondUser2OutboundPojo = new User2();
		secondUser2OutboundPojo.setFavoriteColor("foo" + UUID.randomUUID().toString());
		secondUser2OutboundPojo.setFavoritePlace("foo" + UUID.randomUUID().toString());
		secondUser2OutboundPojo.setName("foo" + UUID.randomUUID().toString());
		source.output().send(MessageBuilder.withPayload(secondUser2OutboundPojo).build());
		Message<?> secondBarOutboundMessage = sourceMessageCollector
				.forChannel(source.output()).poll(1000, TimeUnit.MILLISECONDS);

		ConfigurableApplicationContext sinkContext = SpringApplication.run(
				AvroSinkApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.schemaRegistryClient.enabled=false");
		Sink sink = sinkContext.getBean(Sink.class);
		sink.input().send(outboundMessage);
		sink.input().send(barOutboundMessage);
		sink.input().send(secondBarOutboundMessage);
		List<User1> receivedUsers = sinkContext
				.getBean(AvroSinkApplication.class).receivedUsers;
		assertThat(receivedUsers).hasSize(3);
		assertThat(receivedUsers.get(0)).isNotSameAs(firstOutboundFoo);
		assertThat(receivedUsers.get(0).getFavoriteColor())
				.isEqualTo(firstOutboundFoo.getFavoriteColor());
		assertThat(receivedUsers.get(0).getName()).isEqualTo(firstOutboundFoo.getName());

		assertThat(receivedUsers.get(1)).isNotSameAs(firstOutboundUser2);
		assertThat(receivedUsers.get(1).getFavoriteColor())
				.isEqualTo(firstOutboundUser2.getFavoriteColor());
		assertThat(receivedUsers.get(1).getName())
				.isEqualTo(firstOutboundUser2.getName());

		assertThat(receivedUsers.get(2)).isNotSameAs(secondUser2OutboundPojo);
		assertThat(receivedUsers.get(2).getFavoriteColor())
				.isEqualTo(secondUser2OutboundPojo.getFavoriteColor());
		assertThat(receivedUsers.get(2).getName())
				.isEqualTo(secondUser2OutboundPojo.getName());

		sourceContext.close();
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@ConfigurationProperties
	public static class AvroSourceApplication {

		private Resource schemaLocation;

		@Bean
		public SchemaRegistryClient schemaRegistryClient() {
			return stubSchemaRegistryClient;
		}

		public void setSchemaLocation(Resource schemaLocation) {
			this.schemaLocation = schemaLocation;
		}

		@Bean
		@StreamMessageConverter
		public MessageConverter userMessageConverter() throws IOException {
			AvroSchemaMessageConverter avroSchemaMessageConverter = new AvroSchemaMessageConverter(
					MimeType.valueOf("avro/bytes"));
			if (this.schemaLocation != null) {
				avroSchemaMessageConverter.setSchemaLocation(this.schemaLocation);
			}
			return avroSchemaMessageConverter;
		}

	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	@ConfigurationProperties
	public static class AvroSinkApplication {

		public List<User1> receivedUsers = new ArrayList<>();

		private Resource schemaLocation;

		@StreamListener(Sink.INPUT)
		public void listen(User1 user) {
			this.receivedUsers.add(user);
		}

		public void setSchemaLocation(Resource schemaLocation) {
			this.schemaLocation = schemaLocation;
		}

		@Bean
		@StreamMessageConverter
		public MessageConverter userMessageConverter() throws IOException {
			AvroSchemaMessageConverter avroSchemaMessageConverter = new AvroSchemaMessageConverter(
					MimeType.valueOf("avro/bytes"));
			if (this.schemaLocation != null) {
				avroSchemaMessageConverter.setSchemaLocation(this.schemaLocation);
			}
			return avroSchemaMessageConverter;
		}

	}

}
