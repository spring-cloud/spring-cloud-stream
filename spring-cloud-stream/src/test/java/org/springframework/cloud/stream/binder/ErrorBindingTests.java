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

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.MessageChannel;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;

/**
 * @author Marius Bogoevici
 */
public class ErrorBindingTests {

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testErrorChannelNotBoundByDefault() {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(TestProcessor.class,
				"--server.port=0", "--spring.cloud.stream.default-binder=mock");
		BinderFactory binderFactory = applicationContext.getBean(BinderFactory.class);

		Binder binder = binderFactory.getBinder(null, MessageChannel.class);

		Mockito.verify(binder).bindConsumer(eq("input"), isNull(), any(MessageChannel.class),
				any(ConsumerProperties.class));
		Mockito.verify(binder).bindProducer(eq("output"), any(MessageChannel.class), any(ProducerProperties.class));
		Mockito.verifyNoMoreInteractions(binder);
		applicationContext.close();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestProcessor {

	}
}
