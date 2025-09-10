/*
 * Copyright 2015-present the original author or authors.
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

import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.messaging.MessageChannel;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Marius Bogoevici
 * @author Janne Valkealahti
 * @author Soby Chacko
 */
@SpringBootTest(classes = BindingTargetTests.TestFooChannels.class, properties = {"spring.cloud.stream.default-binder=mock",
	"spring.cloud.function.definition=process1;process2"})
class BindingTargetTests {

	@Autowired
	private BinderFactory binderFactory;

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	void arbitraryInterfaceChannelsBound() {
		Binder binder = this.binderFactory.getBinder(null, MessageChannel.class);
		verify(binder).bindConsumer(eq("someQueue.0"), isNull(),
			Mockito.any(MessageChannel.class), Mockito.any());
		verify(binder).bindConsumer(eq("someQueue.2"), isNull(),
			Mockito.any(MessageChannel.class), Mockito.any());
		verify(binder).bindProducer(eq("someQueue.1"), Mockito.any(MessageChannel.class),
			Mockito.any());
		verify(binder).bindProducer(eq("someQueue.3"), Mockito.any(MessageChannel.class),
			Mockito.any());
		verifyNoMoreInteractions(binder);
	}

	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/arbitrary-binding-test.properties")
	public static class TestFooChannels {

		@Bean
		public Function<String, String> process1() {
			return s -> s;
		}

		@Bean
		public Function<String, String> process2() {
			return s -> s;
		}

	}
}
