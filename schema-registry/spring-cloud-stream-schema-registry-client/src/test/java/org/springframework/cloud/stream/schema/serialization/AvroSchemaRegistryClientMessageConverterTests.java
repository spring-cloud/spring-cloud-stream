/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.cloud.stream.schema.serialization;

import java.io.IOException;
import java.util.UUID;

import example.avro.Command;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.NoOpCache;
import org.springframework.cache.support.NoOpCacheManager;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.cloud.stream.schema.avro.User1;
import org.springframework.cloud.stream.schema.registry.EnableSchemaRegistryServer;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaMessageConverter;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaRegistryClientMessageConverter;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaServiceManager;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaServiceManagerImpl;
import org.springframework.cloud.stream.schema.registry.client.DefaultSchemaRegistryClient;
import org.springframework.cloud.stream.schema.registry.client.EnableSchemaRegistryClient;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.MimeType;
import org.springframework.web.client.RestTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.cloud.stream.schema.serialization.AvroMessageConverterSerializationTests.notification;

/**
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 * @author Sercan Karaoglu
 * @author James Gee
 * @author Christian Tzolov
 */
public class AvroSchemaRegistryClientMessageConverterTests {


	private ConfigurableApplicationContext schemaRegistryServerContext;
	private RestTemplateBuilder restTemplateBuilder;

	@BeforeEach
	public void setup() {
		this.schemaRegistryServerContext = SpringApplication.run(
				ServerApplication.class, "--spring.main.allow-bean-definition-overriding=true");
		this.restTemplateBuilder = this.schemaRegistryServerContext.getBean(RestTemplateBuilder.class);
	}

	@AfterEach
	public void tearDown() {
		this.schemaRegistryServerContext.close();
	}

	@Test
	public void testSendMessage() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(AvroSourceApplication.class))
				.web(WebApplicationType.NONE).run("--server.port=0",
						"--spring.jmx.enabled=false",
						"--spring.cloud.stream.bindings.myBinding-out-0.contentType=application/*+avro",
						"--spring.cloud.schema.avro.dynamicSchemaGenerationEnabled=true")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);

			User1 user1 = new User1();
			user1.setFavoriteColor("foo" + UUID.randomUUID());
			user1.setName("foo" + UUID.randomUUID());

			streamBridge.send("myBinding-out-0", user1);

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive();

			assertThat(result).isNotNull();

			final MessageConverter userMessageConverter = (MessageConverter) context.getBean("userMessageConverter");
			final User1 receivedUser1 = (User1) userMessageConverter.fromMessage(result, User1.class);

			assertThat(receivedUser1).isNotNull();
		}
	}

	@Test
	public void testSchemaImportConfiguration() throws Exception {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(AvroSourceApplication.class))
				.web(WebApplicationType.NONE).run("--server.port=0", "--spring.jmx.enabled=false",
						"--spring.cloud.schema.avro.dynamicSchemaGenerationEnabled=true",
						"--spring.cloud.stream.bindings.foo-out-0.contentType=application/*+avro",
						"--spring.cloud.stream.bindings.output.destination=test",
						"--spring.cloud.stream.bindings.schema-registry-client.endpoint=http://localhost:8990",
						"--spring.cloud.schema.avro.schema-locations=classpath:schemas/Command.avsc",
						"--spring.cloud.schema.avro.schema-imports=classpath:schemas/imports/Sms.avsc,"
								+ " classpath:schemas/imports/Email.avsc, classpath:schemas/imports/PushNotification.avsc")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);

			final Command notification = notification();
			streamBridge.send("foo-out-0", notification);

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive();

			final MessageConverter cmdConverter = (MessageConverter) context.getBean("userMessageConverter");
			final Command command = (Command) cmdConverter.fromMessage(result, Command.class);

			assertThat(command).isNotNull();
		}
	}

	@Test
	public void testNoCacheConfiguration() {
		ConfigurableApplicationContext sourceContext = SpringApplication
				.run(NoCacheConfiguration.class, "--spring.main.web-environment=false");
		AvroSchemaRegistryClientMessageConverter converter = sourceContext
				.getBean(AvroSchemaRegistryClientMessageConverter.class);
		DirectFieldAccessor accessor = new DirectFieldAccessor(converter);
		assertThat(accessor.getPropertyValue("cacheManager")).isInstanceOf(NoOpCacheManager.class);

	}

	@Test
	public void testNamedCacheIsRequested() {
		CacheManager mockCache = Mockito.mock(CacheManager.class);
		when(mockCache.getCache(any())).thenReturn(new NoOpCache(""));
		AvroSchemaServiceManager manager = new AvroSchemaServiceManagerImpl();
		AvroSchemaRegistryClientMessageConverter converter = new AvroSchemaRegistryClientMessageConverter(
				new DefaultSchemaRegistryClient(restTemplateBuilder), mockCache, manager);
		ReflectionTestUtils.invokeMethod(converter, "getCache", "TEST_CACHE");
		verify(mockCache).getCache("TEST_CACHE");
	}

	@EnableAutoConfiguration
	@EnableSchemaRegistryClient
	public static class AvroSourceApplication {

		private Resource schemaLocation;

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

	@Configuration
	public static class NoCacheConfiguration {

		@Bean
		AvroSchemaRegistryClientMessageConverter avroSchemaRegistryClientMessageConverter() {
			AvroSchemaServiceManager manager = new AvroSchemaServiceManagerImpl();
			return new AvroSchemaRegistryClientMessageConverter(
					new DefaultSchemaRegistryClient(new RestTemplate()), new NoOpCacheManager(), manager);
		}

		@Bean
		ServletWebServerFactory servletWebServerFactory() {
			return new TomcatServletWebServerFactory();
		}

	}

	@SpringBootApplication
	@EnableSchemaRegistryServer
	public static class ServerApplication {
		public static void main(String[] args) {
			SpringApplication.run(AvroMessageConverterSerializationTests.ServerApplication.class, args);
		}
	}

}
