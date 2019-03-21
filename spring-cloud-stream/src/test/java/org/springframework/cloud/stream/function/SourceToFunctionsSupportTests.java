/*
 * Copyright 2018-2019 the original author or authors.
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.PollableChannel;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @since 2.1
 */
public class SourceToFunctionsSupportTests {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testFunctionIsAppliedToExistingMessageSource() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						FunctionsConfiguration.class)).web(WebApplicationType.NONE).run(
								"--spring.cloud.stream.function.definition=toUpperCase",
								"--spring.jmx.enabled=false")) {

			OutputDestination target = context.getBean(OutputDestination.class);
			assertThat(target.receive(1000).getPayload())
					.isEqualTo("HELLO FUNCTION".getBytes(StandardCharsets.UTF_8));
		}
	}

	@Test
	public void testComposedFunctionIsAppliedToExistingMessageSource() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						FunctionsConfiguration.class)).web(WebApplicationType.NONE).run(
								"--spring.cloud.stream.function.definition=toUpperCase|concatWithSelf",
								"--spring.jmx.enabled=false")) {
			OutputDestination target = context.getBean(OutputDestination.class);
			assertThat(target.receive(1000).getPayload()).isEqualTo(
					"HELLO FUNCTION:HELLO FUNCTION".getBytes(StandardCharsets.UTF_8));
		}
	}

	@Test
	public void testFailedInputTypeConversion() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						FunctionsConfigurationNoConversionPossible.class))
								.web(WebApplicationType.NONE)
								.run("--spring.cloud.stream.function.definition=toUpperCase|concatWithSelf",
										"--spring.jmx.enabled=false")) {
			PollableChannel errorChannel = context.getBean("errorChannel",
					PollableChannel.class);
			OutputDestination target = context.getBean(OutputDestination.class);
			assertThat(target.receive(1000)).isNull();
			assertThat(errorChannel.receive(10000)).isNotNull();
		}
	}

	@Test
	public void testComposedFunctionIsAppliedToExistingMessageSourceFailedTypeConversion() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						FunctionsConfigurationNoConversionPossible.class))
								.web(WebApplicationType.NONE)
								.run("--spring.cloud.stream.function.definition=toUpperCase|concatWithSelf",
										"--spring.jmx.enabled=false")) {
			PollableChannel errorChannel = context.getBean("errorChannel",
					PollableChannel.class);
			OutputDestination target = context.getBean(OutputDestination.class);
			assertThat(target.receive(1000)).isNull();
			assertThat(errorChannel.receive(10000)).isNotNull();
		}
	}

	@Test
	public void testMessageSourceIsCreatedFromProvidedSupplier() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration
						.getCompleteConfiguration(SupplierConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.cloud.stream.function.definition=number",
										"--spring.jmx.enabled=false")) {

			OutputDestination target = context.getBean(OutputDestination.class);
			assertThat(target.receive(10000).getPayload())
					.isEqualTo("1".getBytes(StandardCharsets.UTF_8));
			assertThat(target.receive(10000).getPayload())
					.isEqualTo("2".getBytes(StandardCharsets.UTF_8));
			assertThat(target.receive(10000).getPayload())
					.isEqualTo("3".getBytes(StandardCharsets.UTF_8));
			// etc
		}
	}

	@Test
	public void testMessageSourceIsCreatedFromProvidedSupplierComposedWithSingleFunction() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						SupplierConfiguration.class)).web(WebApplicationType.NONE).run(
								"--spring.cloud.stream.function.definition=number|concatWithSelf",
								"--spring.jmx.enabled=false")) {

			OutputDestination target = context.getBean(OutputDestination.class);
			assertThat(target.receive(10000).getPayload())
					.isEqualTo("11".getBytes(StandardCharsets.UTF_8));
			assertThat(target.receive(10000).getPayload())
					.isEqualTo("22".getBytes(StandardCharsets.UTF_8));
			assertThat(target.receive(10000).getPayload())
					.isEqualTo("33".getBytes(StandardCharsets.UTF_8));
			// etc
		}
	}

	@Test
	public void testMessageSourceIsCreatedFromProvidedSupplierComposedWithMultipleFunctions() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						SupplierConfiguration.class)).web(WebApplicationType.NONE).run(
								"--spring.cloud.stream.function.definition=number|concatWithSelf|multiplyByTwo",
								"--spring.jmx.enabled=false")) {

			OutputDestination target = context.getBean(OutputDestination.class);
			assertThat(target.receive(10000).getPayload())
					.isEqualTo("22".getBytes(StandardCharsets.UTF_8));
			assertThat(target.receive(10000).getPayload())
					.isEqualTo("44".getBytes(StandardCharsets.UTF_8));
			assertThat(target.receive(10000).getPayload())
					.isEqualTo("66".getBytes(StandardCharsets.UTF_8));
			// etc
		}
	}

	@Test
	public void testFunctionDoesNotExist() {

		this.expectedException.expect(BeanCreationException.class);

		new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(SupplierConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.function.definition=doesNotExist",
								"--spring.jmx.enabled=false");
	}

	@EnableAutoConfiguration
	@Import(ProvidedMessageSourceConfiguration.class)
	public static class SupplierConfiguration {

		AtomicInteger counter = new AtomicInteger();

		@Bean
		public Supplier<String> number() {
			return () -> String.valueOf(this.counter.incrementAndGet());
		}

		@Bean
		public Function<String, String> concatWithSelf() {
			return x -> x + x;
		}

		@Bean
		public Function<Flux<String>, Flux<String>> multiplyByTwo() {
			return x -> x.map(i -> String.valueOf(Integer.valueOf(i) * 2));
		}

	}

	@EnableAutoConfiguration
	@Import(ExistingMessageSourceConfiguration.class)
	public static class FunctionsConfiguration {

		@Bean
		public Function<String, String> toUpperCase() {
			return String::toUpperCase;
		}

		@Bean
		public Function<String, String> concatWithSelf() {
			return x -> x + ":" + x;
		}

	}

	@EnableAutoConfiguration
	@Import(ExistingMessageSourceConfigurationNoContentTypeSet.class)
	public static class FunctionsConfigurationNoConversionPossible {

		@Bean
		public PollableChannel errorChannel() {
			return new QueueChannel(10);
		}

		@Bean
		public Function<Boolean, Boolean> toUpperCase() {
			return x -> true;
		}

		@Bean
		public Function<Boolean, Integer> concatWithSelf() {
			return x -> 1;
		}

	}

	/**
	 * This configuration essentially emulates our existing app-starters for Sources and
	 * essentially demonstrates how a function(s) could be applied to an existing source
	 * via {@link IntegrationFlowFunctionSupport} class.
	 */
	@EnableBinding(Source.class)
	public static class ExistingMessageSourceConfiguration {

		@Autowired
		private Source source;

		@Bean
		public IntegrationFlow messageSourceFlow(
				IntegrationFlowFunctionSupport functionSupport) {
			Supplier<Message<String>> messageSource = () -> MessageBuilder
					.withPayload("hello function")
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
					.build();

			return functionSupport.integrationFlowFromProvidedSupplier(messageSource)
					.channel(this.source.output()).get();
		}

	}

	@EnableBinding(Source.class)
	public static class ExistingMessageSourceConfigurationNoContentTypeSet {

		@Autowired
		private Source source;

		@Bean
		public IntegrationFlow messageSourceFlow(
				IntegrationFlowFunctionSupport functionSupport) {
			Supplier<Message<String>> messageSource = () -> MessageBuilder
					.withPayload("hello function")
					.setHeader(MessageHeaders.CONTENT_TYPE, "application/octet-stream")
					.build();

			return functionSupport.integrationFlowFromProvidedSupplier(messageSource)
					.channel(this.source.output()).get();
		}

	}

	@EnableBinding(Source.class)
	public static class ProvidedMessageSourceConfiguration {

		@Autowired
		private Source source;

		@Autowired
		private StreamFunctionProperties functionProperties;

		@Bean
		public IntegrationFlow messageSourceFlow(
				IntegrationFlowFunctionSupport functionSupport) {
			Assert.hasText(this.functionProperties.getDefinition(),
					"Supplier name must be provided");

			return functionSupport.integrationFlowFromNamedSupplier()
					.channel(this.source.output()).get();
		}

	}

}
