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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.same;

import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.MessageChannel;

/**
 * @author Marius Bogoevici
 */
public class ErrorBindingTests {

	@Test
	public void testErrorChannelNotBoundByDefault() {

		ConfigurableApplicationContext applicationContext = SpringApplication.run(TestProcessor.class, "--server.port=0");
		BinderFactory<?> binderFactory = applicationContext.getBean(BinderFactory.class);

		@SuppressWarnings("unchecked")
		Binder binder = binderFactory.getBinder(null);

		Mockito.verify(binder).bindConsumer(eq("input"), isNull(String.class), any(MessageChannel.class), any(ConsumerProperties.class));
		Mockito.verify(binder).bindProducer(eq("output"), any(MessageChannel.class), any(ProducerProperties.class));
		Mockito.verifyNoMoreInteractions(binder);
		applicationContext.close();
	}

	@Test
	public void testErrorChannelBoundIfConfigured() {

		ConfigurableApplicationContext applicationContext =
				SpringApplication.run(TestProcessor.class, "--spring.cloud.stream.bindings.error.destination=foo", "--server.port=0");
		BinderFactory<?> binderFactory = applicationContext.getBean(BinderFactory.class);

		@SuppressWarnings("unchecked")
		Binder binder =  binderFactory.getBinder(null);

		MessageChannel errorChannel = applicationContext.getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
				MessageChannel.class);

		Mockito.verify(binder).bindConsumer(eq("input"), isNull(String.class), any(MessageChannel.class), any(ConsumerProperties.class));
		Mockito.verify(binder).bindProducer(eq("output"), any(MessageChannel.class), any(ProducerProperties.class));
		Mockito.verify(binder).bindProducer(eq("foo"), same(errorChannel), any(ProducerProperties.class));
		Mockito.verifyNoMoreInteractions(binder);
		applicationContext.close();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	@Import(MockBinderRegistryConfiguration.class)
	public static class TestProcessor {

	}
}
