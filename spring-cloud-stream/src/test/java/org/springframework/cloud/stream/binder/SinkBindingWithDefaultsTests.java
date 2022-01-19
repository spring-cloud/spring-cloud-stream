/*
 * Copyright 2015-2022 the original author or authors.
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

import java.util.function.Consumer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Marius Bogoevici
 * @author Janne Valkealahti
 * @author Soby Chacko
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = SinkBindingWithDefaultsTests.TestSink.class, properties = "spring.cloud.stream.defaultBinder=mock")
public class SinkBindingWithDefaultsTests {

	@Autowired
	private BinderFactory binderFactory;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSinkInputChannelBound() {
		Binder binder = this.binderFactory.getBinder(null, MessageChannel.class);
		verify(binder).bindConsumer(eq("consumer-in-0"), isNull(), Mockito.any(MessageChannel.class),
				Mockito.any());
		verifyNoMoreInteractions(binder);
	}

	@EnableAutoConfiguration
	public static class TestSink {

		@Bean
		public Consumer<String> consumer() {
			return s -> System.out.println();
		}
	}

}
