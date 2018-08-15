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

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
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
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @since 2.1
 *
 */
public class FunctionSupportTests {

	@Test
	public void testFunctionIsAppliedToMessageSource() {
		ApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration.getCompleteConfiguration(MyFunctionsConfiguration.class))
				.web(WebApplicationType.NONE).run("--spring.cloud.stream.function.name=toUpperCase",  "--spring.jmx.enabled=false");

		OutputDestination target = context.getBean(OutputDestination.class);
		assertThat(target.receive(1000).getPayload()).isEqualTo("HELLO FUNCTION".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void testComposedFunctionIsAppliedToMessageSource() {
		ApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration.getCompleteConfiguration(MyFunctionsConfiguration.class))
				.web(WebApplicationType.NONE).run("--spring.cloud.stream.function.name=toUpperCase|concatWithSelf", "--spring.jmx.enabled=false");

		OutputDestination target = context.getBean(OutputDestination.class);
		assertThat(target.receive(1000).getPayload()).isEqualTo("HELLO FUNCTION:HELLO FUNCTION".getBytes(StandardCharsets.UTF_8));
	}

	@SpringBootApplication
	@Import(SourceConfiguration.class)
	public static class MyFunctionsConfiguration {
		@Bean
		public Function<String, String> toUpperCase() {
			return x -> x.toUpperCase();
		}

		@Bean
		public Function<String, String> concatWithSelf() {
			return x -> x + ":" + x;
		}
	}

	/**
	 * This configuration essentially emulates our existing app-startes for Sources
	 * and essentially demonstrates how a function(s) could be applied to an existing
	 * source via {@link FunctionSupport} class.
	 */
	@EnableBinding(Source.class)
	public static class SourceConfiguration {

		@Autowired
		private Source source;

		@Autowired
		private FunctionProperties functionProperties;

		@Bean
		public IntegrationFlow messageSourceFlow(FunctionSupport functionSupport) {
			Supplier<Message<String>> messageSource = () -> MessageBuilder.withPayload("hello function")
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();

			PollerMetadata pm = new PollerMetadata();
			pm.setTrigger(new PeriodicTrigger(1, TimeUnit.SECONDS));
			IntegrationFlowBuilder flowBuilder = IntegrationFlows.from(messageSource);

			if (StringUtils.hasText(functionProperties.getName())) {
				functionSupport.applyFunctionToIntegrationFlow(functionProperties.getName(), flowBuilder, message -> source.output().send(message));
			}
			else {
				flowBuilder = flowBuilder.channel(source.output());
			}

			return flowBuilder.get();
		}
	}
}
