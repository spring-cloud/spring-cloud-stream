/*
 * Copyright 2024-2024 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.context.message.MessageUtils;
import org.springframework.cloud.function.json.JsonMapper;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.test.EnableTestBinder;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.utils.BuildInformationProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Omer Celik
 */
public class HeaderTests {

	@BeforeAll
	public static void before() {
		System.clearProperty("spring.cloud.function.definition");
	}

	@Test
	void checkWithEmptyPojo() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(EmptyConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			Message<EmptyPojo> message = MessageBuilder.withPayload(new EmptyPojo()).build();
			streamBridge.send("emptyConfigurationDestination", message);

			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> messageReceived = outputDestination.receive(1000, "emptyConfigurationDestination");

			checkCommonHeaders(messageReceived.getHeaders());
		}
	}

	@Test
	void checkIfHeaderProvidedInData() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(EmptyConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			String jsonPayload = "{\"name\":\"Omer\"}";
			streamBridge.send("myBinding-out-0",
				MessageBuilder.withPayload(jsonPayload.getBytes())
					.setHeader("anyHeader", "anyValue")
					.build(),
				MimeTypeUtils.APPLICATION_JSON);

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive(1000, "myBinding-out-0");

			checkCommonHeaders(result.getHeaders());
			assertThat(result.getHeaders().get("anyHeader")).isEqualTo("anyValue");
		}
	}

	@Test
	void checkGenericMessageSent() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(FunctionUpperCaseConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=uppercase")) {

			String jsonPayload = "{\"surname\":\"Celik\"}";
			InputDestination input = context.getBean(InputDestination.class);
			input.send(new GenericMessage<>(jsonPayload.getBytes()), "uppercase-in-0");

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive(1000, "uppercase-out-0");

			checkCommonHeaders(result.getHeaders());
		}
	}

	@Test
	void checkGenericMessageSentUsingStreamBridge() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(FunctionUpperCaseConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=uppercase")) {

			String jsonPayload = "{\"anyFieldName\":\"anyValue\"}";
			final StreamBridge streamBridge = context.getBean(StreamBridge.class);
			GenericMessage<String> message = new GenericMessage<>(jsonPayload);
			streamBridge.send("uppercase-in-0", message);

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive(1000, "uppercase-out-0");

			checkCommonHeaders(result.getHeaders());
		}
	}

	@Test
	void checkMessageWrappedFunctionalConsumer() {
		System.clearProperty("spring.cloud.function.definition");
		ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(FunctionMessageConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=uppercase"
			);

		InputDestination source = context.getBean(InputDestination.class);
		source.send(new GenericMessage<>("Omer Celik".getBytes()), "uppercase-in-0");

		OutputDestination target = context.getBean(OutputDestination.class);
		Message<byte[]> message = target.receive(5, "uppercase-out-0");

		checkCommonHeaders(message.getHeaders());
	}

	@Test
	void checkStringToMapMessageStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(
			StringToMapMessageConfiguration.class).web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		String jsonPayload = "{\"name\":\"Omer\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		OutputDestination target = context.getBean(OutputDestination.class);
		Message<byte[]> outputMessage = target.receive();
		checkCommonHeaders(outputMessage.getHeaders());
	}

	@Test
	void checkPojoToPojo() {
		ApplicationContext context = new SpringApplicationBuilder(
			PojoToPojoConfiguration.class).web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		String jsonPayload = "{\"name\":\"Omer\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		OutputDestination target = context.getBean(OutputDestination.class);
		Message<byte[]> outputMessage = target.receive();
		checkCommonHeaders(outputMessage.getHeaders());
	}

	@Test
	void checkPojoToString() {
		ApplicationContext context = new SpringApplicationBuilder(
			PojoToStringConfiguration.class).web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"Neso\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		checkCommonHeaders(outputMessage.getHeaders());
	}

	@Test
	void checkPojoToByteArray() {
		ApplicationContext context = new SpringApplicationBuilder(
			PojoToByteArrayConfiguration.class).web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"Neptune\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		checkCommonHeaders(outputMessage.getHeaders());
	}

	@Test
	void checkStringToPojoInboundContentTypeHeader() {
		ApplicationContext context = new SpringApplicationBuilder(
			StringToPojoConfiguration.class).web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"Mercury\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes(),
			new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
				MimeTypeUtils.APPLICATION_JSON_VALUE))));
		Message<byte[]> outputMessage = target.receive();
		checkCommonHeaders(outputMessage.getHeaders());
	}

	@Test
	void checkPojoMessageToStringMessage() {
		ApplicationContext context = new SpringApplicationBuilder(
			PojoMessageToStringMessageConfiguration.class)
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"Earth\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		MessageHeaders headers = outputMessage.getHeaders();
		assertThat(BuildInformationProvider.isVersionValid((String) headers.get(BinderHeaders.SCST_VERSION))).isTrue();
	}

	private void checkCommonHeaders(MessageHeaders headers) {
		assertThat(headers).isNotNull();
		assertThat(headers.get(MessageUtils.TARGET_PROTOCOL)).isEqualTo("kafka");
		assertThat(headers.get(MessageHeaders.CONTENT_TYPE)).isEqualTo("application/json");
		assertThat(BuildInformationProvider.isVersionValid((String) headers.get(BinderHeaders.SCST_VERSION))).isTrue();
	}

	@EnableAutoConfiguration
	public static class EmptyConfiguration {

	}

	@EnableAutoConfiguration
	public static class FunctionMessageConfiguration {
		@Bean
		public Function<Message<String>, Message<String>> uppercase() {
			return msg -> MessageBuilder.withPayload(msg.getPayload().toUpperCase(Locale.ROOT)).build();
		}
	}

	@EnableAutoConfiguration
	@Configuration
	public static class FunctionUpperCaseConfiguration {
		@Bean
		public Function<String, String> uppercase() {
			return String::toUpperCase;
		}
	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class StringToMapMessageConfiguration {
		@Bean
		public Function<Message<Map<?, ?>>, String> echo() {
			return value -> {
				assertThat(value.getPayload() instanceof Map).isTrue();
				return (String) value.getPayload().get("name");
			};
		}
	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class PojoToPojoConfiguration {

		@Bean
		public Function<Planet, Planet> echo() {
			return value -> value;
		}
	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class PojoToStringConfiguration {

		@Bean
		public Function<Planet, String> echo() {
			return Planet::toString;
		}
	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class PojoToByteArrayConfiguration {

		@Bean
		public Function<Planet, byte[]> echo() {
			return value -> value.toString().getBytes(StandardCharsets.UTF_8);
		}
	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class StringToPojoConfiguration {

		@Bean
		public Function<String, Planet> echo(JsonMapper mapper) {
			return value -> mapper.fromJson(value, Planet.class);
		}
	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class PojoMessageToStringMessageConfiguration {

		@Bean
		public Function<Message<Planet>, Message<String>> echo() {
			return value -> MessageBuilder.withPayload(value.getPayload().toString())
				.setHeader("expected-content-type", MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();
		}
	}

	public static class Planet {

		private String name;

		Planet() {
			this(null);
		}

		Planet(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return this.name;
		}

	}

	public static class EmptyPojo {

	}
}
