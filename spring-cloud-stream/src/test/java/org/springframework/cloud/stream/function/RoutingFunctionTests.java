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

import java.util.function.Function;

import org.junit.After;
import org.junit.Test;
import reactor.core.publisher.Flux;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeTypeUtils;



import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 * @since 2.2.1
 */
public class RoutingFunctionTests {

	@After
	public void after() {
		System.getProperties().remove("spring.cloud.function.routing.enabled");
		System.getProperties().remove("spring.cloud.stream.function.definition");
	}

	@Test
	public void testDefaultRoutingFunctionBinding() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						RoutingFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.function.routing.enabled=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes())
					.setHeader("function.name", "echo")
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
					.build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	public void testDefaultRoutingFunctionBindingFlux() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						RoutingFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.function.routing.enabled=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes())
					.setHeader("function.name", "echoFlux")
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
					.build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test // see RoutingFunctionEnvironmentPostProcessor
	public void testExplicitRoutingFunctionBinding() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						RoutingFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=router")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes())
					.setHeader("function.name", "echo")
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
					.build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	public void testExplicitRoutingFunctionBindingWithCompositionAndRoutingEnabledExplicitly() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						RoutingFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=enrich|router",
										"--spring.cloud.function.routing.enabled=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("HELLO".getBytes());
		}
	}

	@Test
	public void testExplicitRoutingFunctionBindingWithCompositionAndRoutingEnabledImplicitly() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						RoutingFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=enrich|router")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("HELLO".getBytes());
		}
	}

	@Test
	public void testExplicitRoutingFunctionBindingWithCompositionAndRoutingEnabledExplicitlyAndMoreComposition() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						RoutingFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=enrich|router|reverse",
										"--spring.cloud.function.routing.enabled=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("OLLEH".getBytes());
		}
	}

	@Test
	public void testExplicitRoutingFunctionBindingWithCompositionAndRoutingEnabledImplicitlyAndMoreComposition() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						RoutingFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=enrich|router|reverse")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("OLLEH".getBytes());
		}
	}



	@EnableAutoConfiguration
	public static class RoutingFunctionConfiguration  {

		@Bean
		public Function<String, String> echo() {
			return x -> {
				System.out.println("===> echo");
				return x;
			};
		}

		@Bean
		public Function<Flux<String>, Flux<String>> echoFlux() {
			return flux -> flux.map(x -> {
				System.out.println("===> echoFlux");
				return x;
			});
		}

		@Bean
		public Function<Message<String>, Message<String>> enrich() {
			return x -> {
				System.out.println("===> enrich");
				return MessageBuilder.withPayload(x.getPayload()).setHeader("function.name", "uppercase").build();
			};
		}

		@Bean
		public Function<String, String> uppercase() {
			return x -> {
				System.out.println("===> uppercase");
				return x.toUpperCase();
			};
		}

		@Bean
		public Function<String, String> reverse() {
			return x -> {
				System.out.println("===> reverse");
				return new StringBuilder(x).reverse().toString();
			};
		}
	}
}
