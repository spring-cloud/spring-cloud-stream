/*
 * Copyright 2016-2017 the original author or authors.
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

import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class StreamListenerDuplicateMappingTests {

	@Test
	@SuppressWarnings("unchecked")
	public void testMultipleMappingsWithReturnValue() {
		ConfigurableApplicationContext context = null;
		try {
			context = SpringApplication.run(TestMultipleMappingsWithReturnValue.class,
					"--server.port=0");
			fail("Exception expected on duplicate mapping");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).startsWith(
					StreamListenerErrorMessages.MULTIPLE_VALUE_RETURNING_METHODS);
		}
		finally {
			if (context != null) {
				context.close();
			}
		}
	}

	@Test
	public void testDuplicateMappingFromAbstractMethod() {
		ConfigurableApplicationContext context = null;
		try {
			context = SpringApplication.run(TestDuplicateMappingFromAbstractMethod.class,
					"--server.port=0");
		}
		catch (BeanCreationException e) {
			String errorMessage = e.getCause().getMessage()
					.startsWith("Duplicate @StreamListener mapping")
							? "Duplicate mapping exception is not expected"
							: "Test failed with exception";
			fail(errorMessage + ": " + e.getMessage());
		}
		finally {
			if (context != null) {
				context.close();
			}
		}
	}

	public interface GenericSink<T extends Base> {

		void testMethod(T msg);

	}

	public interface Base {

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestMultipleMappingsWithReturnValue {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String receive(Message<String> fooMessage) {
			return null;
		}

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String receiveDuplicateMapping(Message<String> fooMessage) {
			return null;
		}

	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestDuplicateMappingFromAbstractMethod
			implements GenericSink<TestBase> {

		@Override
		@StreamListener(Sink.INPUT)
		public void testMethod(TestBase msg) {
		}

	}

	public class TestBase implements Base {

	}

}
