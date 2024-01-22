/*
 * Copyright 2017-2024 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 */
public class BindingServiceConfigurationTests {

	@Test
	void testErroChannelDistributesMessagesInCaseOfException() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration
				.getCompleteConfiguration(EmptyConfiguration.class))
			.web(WebApplicationType.NONE).run()) {
			AtomicInteger counter = new AtomicInteger();
			PublishSubscribeChannel channel = context.getBean("errorChannel", PublishSubscribeChannel.class);
			//channel.setIgnoreFailures(true);
			channel.subscribe(m -> {
				counter.incrementAndGet();
				throw new RuntimeException("one");
			});
			channel.subscribe(m -> {
				counter.incrementAndGet();
				throw new RuntimeException("two");
			});
			channel.subscribe(m -> {
				counter.incrementAndGet();
				throw new RuntimeException("three");
			});
			channel.send(new GenericMessage<String>("foo"));
			assertThat(counter.get()).isEqualTo(3);
		}
	}

	@Test
	void valdateImportedConfiguartionHandlerPostProcessing() {
		ApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration
				.getCompleteConfiguration(RootConfiguration.class))
			.web(WebApplicationType.NONE).run();
		Map<String, AbstractReplyProducingMessageHandler> beansOfType = context
			.getBeansOfType(AbstractReplyProducingMessageHandler.class);
		for (AbstractReplyProducingMessageHandler handler : beansOfType.values()) {
			assertThat(handler.getNotPropagatedHeaders().contains("contentType"))
				.isTrue();
		}
	}

	@Test // See: https://github.com/spring-cloud/spring-cloud-stream/issues/2883
	void customErrorChannelDoesNotThrowExceptions() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration
				.getCompleteConfiguration(CustomErrorChannelConfiguration.class))
			.web(WebApplicationType.NONE).run()) {
			DirectChannel channel = context.getBean("errorChannel", DirectChannel.class);
			assertThat(channel).isNotNull();
		}
	}

	@Configuration
	@Import(ImportedConfiguration.class)
	public static class RootConfiguration {

		@ServiceActivator(inputChannel = "input")
		public void rootService(String val) {
		}

	}

	@Configuration
	public static class ImportedConfiguration {

		@ServiceActivator(inputChannel = "input")
		public void importedService(String val) {
		}

	}

	@Configuration
	public static class EmptyConfiguration {

	}

	@Configuration
	public static class CustomErrorChannelConfiguration {

		@Bean
		public DirectChannel errorChannel() {
			return new DirectChannel();
		}
	}

}
