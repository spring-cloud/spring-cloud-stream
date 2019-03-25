/*
 * Copyright 2019-2019 the original author or authors.
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

import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class ImplicitFunctionBindingTests {

	@Test
	public void testBindingWithNoEnableBindingConfiguration() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						NoEnableBindingConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	public void testBindingWithEnableBindingConfiguration() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						EnableBindingConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	public void testBindingWithNoEnableBindingAndNoDefinitionPropertyConfiguration() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						SingleFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@EnableAutoConfiguration
	public static class NoEnableBindingConfiguration  {

		@Bean
		public Function<String, String> func() {
			return x -> {
				System.out.println("Function");
				return x;
			};
		}

		@Bean
		public Consumer<String> cons() {
			return x -> {
				System.out.println("Consumer");
			};
		}
	}

	@EnableAutoConfiguration
	@EnableBinding(Processor.class)
	public static class EnableBindingConfiguration  {

		@Bean
		public Function<String, String> func() {
			return x -> {
				System.out.println("Function");
				return x;
			};
		}

		@Bean
		public Consumer<String> cons() {
			return x -> {
				System.out.println("Consumer");
			};
		}
	}

	@EnableAutoConfiguration
	public static class SingleFunctionConfiguration {

		@Bean
		public Function<String, String> func() {
			return x -> {
				System.out.println("Function");
				return x;
			};
		}

	}

}
