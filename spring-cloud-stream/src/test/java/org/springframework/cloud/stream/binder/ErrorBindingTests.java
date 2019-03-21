/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;

/**
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 */
public class ErrorBindingTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testErrorChannelNotBoundByDefault() {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				TestProcessor.class, "--server.port=0",
				"--spring.cloud.stream.default-binder=mock",
				"--spring.jmx.enabled=false");
		BinderFactory binderFactory = applicationContext.getBean(BinderFactory.class);

		Binder binder = binderFactory.getBinder(null, MessageChannel.class);

		Mockito.verify(binder).bindConsumer(eq("input"), isNull(),
				any(MessageChannel.class), any(ConsumerProperties.class));
		Mockito.verify(binder).bindProducer(eq("output"), any(MessageChannel.class),
				any(ProducerProperties.class));
		Mockito.verifyNoMoreInteractions(binder);
		applicationContext.close();
	}

	@Test
	public void testConfigurationWithDefaultErrorHandler() {
		ApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ErrorBindingTests.ErrorConfigurationDefault.class))
								.web(WebApplicationType.NONE)
								.run("--spring.cloud.stream.bindings.input.consumer.max-attempts=1",
										"--spring.jmx.enabled=false");

		InputDestination source = context.getBean(InputDestination.class);
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));

		ErrorConfigurationDefault errorConfiguration = context
				.getBean(ErrorConfigurationDefault.class);
		assertThat(errorConfiguration.counter == 3);
	}

	@Test
	public void testConfigurationWithCustomErrorHandler() {
		ApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ErrorBindingTests.ErrorConfigurationWithCustomErrorHandler.class))
								.web(WebApplicationType.NONE)
								.run("--spring.cloud.stream.bindings.input.consumer.max-attempts=1",
										"--spring.jmx.enabled=false");

		InputDestination source = context.getBean(InputDestination.class);
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));

		ErrorConfigurationWithCustomErrorHandler errorConfiguration = context
				.getBean(ErrorConfigurationWithCustomErrorHandler.class);
		assertThat(errorConfiguration.counter == 6);
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestProcessor {

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class ErrorConfigurationDefault {

		private int counter;

		@StreamListener(Sink.INPUT)
		public void handle(Object value) {
			this.counter++;
			throw new RuntimeException("BOOM!");
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class ErrorConfigurationWithCustomErrorHandler {

		private int counter;

		@StreamListener(Sink.INPUT)
		public void handle(Object value) {
			this.counter++;
			throw new RuntimeException("BOOM!");
		}

		@ServiceActivator(inputChannel = "input.anonymous.errors")
		public void error(Message<?> message) {
			this.counter++;
		}

	}

}
