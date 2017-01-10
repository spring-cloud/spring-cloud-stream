/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.config;

import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class StreamListenerDuplicateMappingTests {

	@Test
	@SuppressWarnings("unchecked")
	public void testDuplicateMapping() {
		ConfigurableApplicationContext context = null;
		try {
			context = SpringApplication.run(TestDuplicateMapping.class, "--server.port=0");
			fail("Exception expected on duplicate mapping");
		}
		catch (BeanCreationException e) {
			assertThat(e.getCause().getMessage()).startsWith("Duplicate @StreamListener mapping");
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
			context = SpringApplication.run(TestDuplicateMappingFromAbstractMethod.class, "--server.port=0");
		}
		catch (BeanCreationException e) {
			String errorMessage = e.getCause().getMessage().startsWith("Duplicate @StreamListener mapping") ?
					"Duplicate mapping exception is not expected" : "Test failed with exception";
			fail(errorMessage + ": " + e.getMessage());
		}
		finally {
			if (context != null) {
				context.close();
			}
		}
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestDuplicateMapping {

		@StreamListener(Sink.INPUT)
		public void receive(Message<String> fooMessage) {
		}

		@StreamListener(Processor.INPUT)
		public void receiveDuplicateMapping(Message<String> fooMessage) {
		}
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestDuplicateMappingFromAbstractMethod implements ISink<TestBase> {

		@Override
		@StreamListener(Sink.INPUT)
		public void testMethod(TestBase msg) {
		}
	}

	public interface ISink<T extends Base> {
		void testMethod(T msg);
	}

	public interface Base {

	}

	public class TestBase implements Base {

	}

}
