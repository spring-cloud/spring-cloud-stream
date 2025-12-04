/*
 * Copyright 2019-present the original author or authors.
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

import java.lang.reflect.Field;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.cloud.function.context.config.RoutingFunction;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 * @since 2.2.1
 */
class RoutingFunctionTests {


	@BeforeAll
	public static void before() {
		System.getProperties().remove("spring.cloud.function.routing.enabled");
		System.getProperties().remove("spring.cloud.stream.function.definition");
		System.getProperties().remove("spring.cloud.function.definition");
		System.getProperties().remove("spring.cloud.function.routing-expression");
	}

	@Test
	void routingViaExplicitEnablingAndDefinitionHeader() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.routing.enabled=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
				.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
				.withPayload("Hello".getBytes())
				.setHeader(FunctionProperties.PREFIX + ".definition", "echo")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());
		}
	}

	@Test
	void routingViaExplicitEnablingAndRoutingExpressionProperty() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.routing-expression=headers.contentType.toString().equals('text/plain') ? 'echo' : null",
				"--spring.cloud.stream.function.routing.enabled=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
				.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
				.withPayload("Hello".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());
		}
	}

	@Test
	void routingViaExplicitEnablingAndRoutingExpressionHeader() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.routing.enabled=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
				.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
				.withPayload("Hello".getBytes())
				.setHeader("spring.cloud.function.routing-expression", "'echo'")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());
		}
	}

	@Test
	void routingViaExplicitDefinitionAndDefinitionHeader() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=" + RoutingFunction.FUNCTION_NAME)) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
				.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
				.withPayload("Hello".getBytes())
				.setHeader("spring.cloud.function.definition", "echo|uppercase")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("HELLO".getBytes());
		}
	}

	@Test
	void defaultRoutingFunctionBindingFlux() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.routing.enabled=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
				.withPayload("Hello".getBytes())
				.setHeader("spring.cloud.function.definition", "echoFlux")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build();
			inputDestination.send(inputMessage);

			TestChannelBinder binder = context.getBean(TestChannelBinder.class);
			Throwable ex = ((Exception) binder.getLastError().getPayload()).getCause();
			assertThat(ex).isInstanceOf(IllegalStateException.class);
			assertThat(ex.getMessage()).isEqualTo("Routing to functions that return Publisher is not supported in the context of Spring Cloud Stream.");
		}
	}


	@Test
	void pojoFunction() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=" + RoutingFunction.FUNCTION_NAME)) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
				.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
				.withPayload("{\"name\":\"bob\"}".getBytes())
				.setHeader("spring.cloud.function.definition", "pojoecho")
				.build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("{\"name\":\"bob\"}".getBytes());

		}
	}

	@Test
	void explicitRoutingFunctionBindingWithCompositionAndRoutingEnabledExplicitly() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=enrich|" + RoutingFunction.FUNCTION_NAME,
				"--spring.cloud.stream.function.routing.enabled=true")) {

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
	void explicitRoutingFunctionBindingWithCompositionAndRoutingEnabledImplicitly() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=enrich|" + RoutingFunction.FUNCTION_NAME)) {

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
	void explicitRoutingFunctionBindingWithCompositionAndRoutingEnabledExplicitlyAndMoreComposition() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=enrich|" + RoutingFunction.FUNCTION_NAME + "|reverse",
				"--spring.cloud.stream.function.routing.enabled=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
				.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
				.withPayload("Hello".getBytes())
				.build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("OLLEH".getBytes());
		}
	}

	@Test
	void explicitRoutingFunctionBindingWithCompositionAndRoutingEnabledImplicitlyAndMoreComposition() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=enrich|" + RoutingFunction.FUNCTION_NAME + "|reverse")) {

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

	@SuppressWarnings("unchecked")
	@Test
	void routingToConsumers() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				RoutingConsumerConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.routing-expression=headers['func_name']")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			Message<byte[]> inputMessage = MessageBuilder
				.withPayload("foo".getBytes())
				.setHeader("func_name", "consume")
				.build();
			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Field chField = ReflectionUtils.findField(outputDestination.getClass(), "channels");
			chField.setAccessible(true);
			List<AbstractSubscribableChannel> outputChannels = (List<AbstractSubscribableChannel>) chField.get(outputDestination);
			assertThat(outputChannels.isEmpty());
			inputDestination.send(inputMessage);
			assertThat(outputChannels.isEmpty());
			inputMessage = MessageBuilder
				.withPayload("foo".getBytes())
				.setHeader("func_name", "echo")
				.build();
			inputDestination.send(inputMessage);
			assertThat(outputChannels.size()).isEqualTo(1);
		}
	}

	@EnableAutoConfiguration
	public static class RoutingConsumerConfiguration  {
		@Bean
		public Consumer<String> consume() {
			return System.out::println;
		}
		@Bean
		public Function<String, String> echo() {
			return x -> x;
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
		public Function<Person, Person> pojoecho() {
			return x -> {
				System.out.println("===> pojoecho");
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
				return MessageBuilder.withPayload(x.getPayload()).setHeader("spring.cloud.function.definition", "uppercase").build();
			};
		}

		@Bean
		public Function<String, String> uppercase() {
			return x -> {
				return x.toUpperCase(Locale.ROOT);
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

	@SuppressWarnings("unused")
	private static class Person {
		private String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}
