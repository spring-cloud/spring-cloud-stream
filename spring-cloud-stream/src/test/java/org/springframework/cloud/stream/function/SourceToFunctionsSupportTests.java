/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;

import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @since 2.1
 *
 */
public class SourceToFunctionsSupportTests {

	@Test
	public void testFunctionIsAppliedToExistingMessageSource() {
		ApplicationContext context =
				new SpringApplicationBuilder(
						TestChannelBinderConfiguration.getCompleteConfiguration(FunctionsConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.function.name=toUpperCase", "--spring.jmx.enabled=false");

		OutputDestination target = context.getBean(OutputDestination.class);
		assertThat(target.receive(1000).getPayload()).isEqualTo("HELLO FUNCTION".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void testComposedFunctionIsAppliedToExistingMessageSource() {
		ApplicationContext context =
				new SpringApplicationBuilder(
						TestChannelBinderConfiguration.getCompleteConfiguration(FunctionsConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.function.name=toUpperCase|concatWithSelf",
								"--spring.jmx.enabled=false");

		OutputDestination target = context.getBean(OutputDestination.class);
		assertThat(target.receive(1000).getPayload())
				.isEqualTo("HELLO FUNCTION:HELLO FUNCTION".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void testMessageSourceIsCreatedFromProvidedSupplier() {
		ApplicationContext context =
				new SpringApplicationBuilder(
						TestChannelBinderConfiguration.getCompleteConfiguration(SupplierConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.function.name=number",
								"--spring.jmx.enabled=false");

		OutputDestination target = context.getBean(OutputDestination.class);
		assertThat(target.receive(10000).getPayload())
				.isEqualTo("1".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(10000).getPayload())
				.isEqualTo("2".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(10000).getPayload())
				.isEqualTo("3".getBytes(StandardCharsets.UTF_8));
		//etc
	}

	@Test
	public void testMessageSourceIsCreatedFromProvidedSupplierComposedWithSingleFunction() {
		ApplicationContext context =
				new SpringApplicationBuilder(
						TestChannelBinderConfiguration.getCompleteConfiguration(SupplierConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.function.name=number|concatWithSelf",
								"--spring.jmx.enabled=false");

		OutputDestination target = context.getBean(OutputDestination.class);
		assertThat(target.receive(10000).getPayload())
				.isEqualTo("11".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(10000).getPayload())
				.isEqualTo("22".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(10000).getPayload())
				.isEqualTo("33".getBytes(StandardCharsets.UTF_8));
		//etc
	}

	@Test
	public void testMessageSourceIsCreatedFromProvidedSupplierComposedWithMultipleFunctions() {
		ApplicationContext context =
				new SpringApplicationBuilder(
						TestChannelBinderConfiguration.getCompleteConfiguration(SupplierConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.function.name=number|concatWithSelf|mmultiplyByTwo",
								"--spring.jmx.enabled=false");

		OutputDestination target = context.getBean(OutputDestination.class);
		assertThat(target.receive(10000).getPayload())
				.isEqualTo("22".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(10000).getPayload())
				.isEqualTo("44".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(10000).getPayload())
				.isEqualTo("66".getBytes(StandardCharsets.UTF_8));
		//etc
	}

	@EnableAutoConfiguration
	@Import(ProvidedMessageSourceConfiguration.class)
	public static class SupplierConfiguration {
		AtomicInteger counter = new AtomicInteger();
		@Bean
		public Supplier<String> number() {
			return () -> String.valueOf(counter.incrementAndGet());
		}

		@Bean
		public Function<String, String> concatWithSelf() {
			return x -> x + x;
		}

		@Bean
		public Function<Flux<String>, Flux<String>> mmultiplyByTwo() {
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

	/**
	 * This configuration essentially emulates our existing app-starters for Sources
	 * and essentially demonstrates how a function(s) could be applied to an existing
	 * source via {@link IntegrationFlowFunctionSupport} class.
	 */
	@EnableBinding(Source.class)
	public static class ExistingMessageSourceConfiguration {

		@Autowired
		private Source source;

		@Bean
		public IntegrationFlow messageSourceFlow(IntegrationFlowFunctionSupport functionSupport) {
			Supplier<Message<String>> messageSource = () -> MessageBuilder.withPayload("hello function")
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();

			IntegrationFlowBuilder flowBuilder = functionSupport.integrationFlowFromProvidedSupplier(messageSource);

			if (!functionSupport.andThenFunction(flowBuilder, source.output())) {
				flowBuilder = flowBuilder.channel(source.output());
			}

			return flowBuilder.get();
		}

	}

	@EnableBinding(Source.class)
	public static class ProvidedMessageSourceConfiguration {

		@Autowired
		private Source source;

		@Autowired
		private FunctionProperties functionProperties;

		@Bean
		public IntegrationFlow messageSourceFlow(IntegrationFlowFunctionSupport functionSupport) {
			Assert.hasText(functionProperties.getName(), "Supplier name must be provided");

			IntegrationFlowBuilder flowBuilder = functionSupport
					.integrationFlowFromNamedSupplier()
					.split()
					.channel(source.output());

			return flowBuilder.get();
		}

	}

}
