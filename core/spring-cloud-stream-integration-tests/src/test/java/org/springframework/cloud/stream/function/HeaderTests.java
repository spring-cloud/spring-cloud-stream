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

import java.util.Locale;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.context.message.MessageUtils;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
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
			MessageHeaders headers = messageReceived.getHeaders();
			assertThat(headers).isNotNull();
			assertThat(headers.get(MessageUtils.TARGET_PROTOCOL)).isEqualTo("kafka");
			assertThat(headers.get(MessageHeaders.CONTENT_TYPE)).isEqualTo("application/json");
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
			MessageHeaders headers = result.getHeaders();
			assertThat(headers).isNotNull();
			assertThat(headers.get(MessageUtils.TARGET_PROTOCOL)).isEqualTo("kafka");
			assertThat(headers.get(MessageHeaders.CONTENT_TYPE)).isEqualTo("application/json");
			assertThat(headers.get("anyHeader")).isEqualTo("anyValue");
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
			MessageHeaders headers = result.getHeaders();
			assertThat(headers).isNotNull();
			assertThat(headers.get(MessageUtils.TARGET_PROTOCOL)).isEqualTo("kafka");
			assertThat(headers.get(MessageHeaders.CONTENT_TYPE)).isEqualTo("application/json");
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
		MessageHeaders headers = message.getHeaders();
		assertThat(headers).isNotNull();
		assertThat(headers).isNotNull();
		assertThat(headers.get(MessageHeaders.CONTENT_TYPE)).isEqualTo("application/json");
		assertThat(headers.get(MessageUtils.TARGET_PROTOCOL)).isEqualTo("kafka");
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

	public static class EmptyPojo {

	}
}
