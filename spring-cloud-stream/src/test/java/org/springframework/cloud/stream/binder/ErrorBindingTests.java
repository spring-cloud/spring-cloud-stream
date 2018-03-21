/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;

/**
 * @author Marius Bogoevici
 */
public class ErrorBindingTests {

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testErrorChannelNotBoundByDefault() {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(TestProcessor.class,
				"--server.port=0");
		BinderFactory binderFactory = applicationContext.getBean(BinderFactory.class);

		Binder binder = binderFactory.getBinder(null, MessageChannel.class);

		Mockito.verify(binder).bindConsumer(eq("input"), isNull(), any(MessageChannel.class),
				any(ConsumerProperties.class));
		Mockito.verify(binder).bindProducer(eq("output"), any(MessageChannel.class), any(ProducerProperties.class));
		Mockito.verifyNoMoreInteractions(binder);
		applicationContext.close();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testErrorChannelBoundIfConfigured() {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(TestProcessor.class,
				"--spring.cloud.stream.bindings.error.destination=foo", "--server.port=0");
		BinderFactory binderFactory = applicationContext.getBean(BinderFactory.class, MessageChannel.class);

		Binder binder = binderFactory.getBinder(null, MessageChannel.class);

		MessageChannel errorChannel = applicationContext.getBean(BindingServiceConfiguration.ERROR_BRIDGE_CHANNEL,
				MessageChannel.class);

		Mockito.verify(binder).bindConsumer(eq("input"), isNull(), any(MessageChannel.class),
				any(ConsumerProperties.class));
		Mockito.verify(binder).bindProducer(eq("output"), any(MessageChannel.class), any(ProducerProperties.class));
		Mockito.verify(binder).bindProducer(eq("foo"), same(errorChannel), any(ProducerProperties.class));
		Mockito.verifyNoMoreInteractions(binder);
		applicationContext.close();
	}

	@Test
	public void testErrorChannelIsBoundWithCorrectContentTypeConverter() {
		final AtomicBoolean received = new AtomicBoolean(false);
		ConfigurableApplicationContext applicationContext = SpringApplication.run(TestProcessor.class,
				"--spring.cloud.stream.bindings.error.destination=foo",
				"--spring.cloud.stream.bindings.error.content-type=application/json",
				"--server.port=0");

		MessageChannel errorChannel = applicationContext.getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
				MessageChannel.class);
		MessageChannel errorBridgeChannel = applicationContext.getBean(BindingServiceConfiguration.ERROR_BRIDGE_CHANNEL,
				MessageChannel.class);

		((SubscribableChannel) errorBridgeChannel).subscribe(message -> {
			assertThat(new String((byte[]) message.getPayload())).isEqualTo("{\"foo\":\"bar\"}");
			received.set(true);
		});

		Foo foo = new Foo();
		foo.setFoo("bar");

		errorChannel.send(new GenericMessage<>(foo));
		assertThat(received.get()).isTrue();
		applicationContext.close();
	}

	@Test
	public void testErrorChannelForExceptionWhenContentTypeIsSet() {
		final AtomicBoolean received = new AtomicBoolean(false);
		ConfigurableApplicationContext applicationContext = SpringApplication.run(TestProcessor.class,
				"--spring.cloud.stream.bindings.error.destination=foo",
				"--spring.cloud.stream.bindings.error.content-type=application/json",
				"--server.port=0");

		MessageChannel errorChannel = applicationContext.getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
				MessageChannel.class);
		MessageChannel errorBridgeChannel = applicationContext.getBean(BindingServiceConfiguration.ERROR_BRIDGE_CHANNEL,
				MessageChannel.class);

		((SubscribableChannel) errorBridgeChannel).subscribe(message -> {
			String payload = new String((byte[]) message.getPayload());
			assertThat(payload.contains("cause")).isTrue();
			assertThat(payload.contains("stackTrace")).isTrue();
			assertThat(payload.contains("throwing exception")).isTrue();
			received.set(true);
		});

		errorChannel.send(new GenericMessage<>(new Exception("throwing exception")));
		assertThat(received.get()).isTrue();
		applicationContext.close();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	@Import(MockBinderRegistryConfiguration.class)
	public static class TestProcessor {

	}

	private class Foo {
		String foo;

		@SuppressWarnings("unused") // used json ser/deser
		public String getFoo() {
			return foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}
	}
}
