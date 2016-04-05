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

package org.springframework.cloud.stream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * @author Ilayaperumal Gopinathan
 */
public class PollableChannelBindingTests {

	@Test
	public void testPollableChannelBinding() {

		ConfigurableApplicationContext applicationContext = SpringApplication.run(TestSource.class, "--server.port=0");
		BinderFactory<?> binderFactory = applicationContext.getBean(BinderFactory.class);
		@SuppressWarnings("unchecked")
		Binder binder = (Binder) binderFactory.getBinder(null);
		Mockito.verify(binder).bindProducer(eq("pollableOutput"), any(SubscribableChannel.class), any(ProducerProperties.class));
		Mockito.verifyNoMoreInteractions(binder);
		applicationContext.close();
	}

	@EnableBinding(PollableSource.class)
	@EnableAutoConfiguration
	@Import(MockBinderRegistryConfiguration.class)
	public static class TestSource {

	}

	private interface PollableSource {

		String OUTPUT = "pollableOutput";

		@Output(PollableSource.OUTPUT)
		PollableChannel output();
	}
}
