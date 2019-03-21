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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 *
 */
@RunWith(StreamListenerMethodReturnWithConversionTests.class)
@Suite.SuiteClasses({
		StreamListenerMethodReturnWithConversionTests.TestReturnConversion.class,
		StreamListenerMethodReturnWithConversionTests.TestReturnNoConversion.class })
public class StreamListenerMethodReturnWithConversionTests extends Suite {

	public StreamListenerMethodReturnWithConversionTests(Class<?> klass,
			RunnerBuilder builder) throws InitializationError {
		super(klass, builder);
	}

	@RunWith(Parameterized.class)
	public static class TestReturnConversion {

		private Class<?> configClass;

		public TestReturnConversion(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection<?> InputConfigs() {
			return Arrays.asList(new Class[] { TestPojoWithMimeType1.class,
					TestPojoWithMimeType2.class });
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testReturnConversion() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(
					this.configClass,
					"--spring.cloud.stream.bindings.output.contentType=application/json",
					"--server.port=0", "--spring.jmx.enabled=false");
			MessageCollector collector = context.getBean(MessageCollector.class);
			Processor processor = context.getBean(Processor.class);
			String id = UUID.randomUUID().toString();
			processor.input()
					.send(MessageBuilder.withPayload("{\"foo\":\"barbar" + id + "\"}")
							.setHeader("contentType", "application/json").build());
			TestPojoWithMimeType testPojoWithMimeType = context
					.getBean(TestPojoWithMimeType.class);
			Assertions.assertThat(testPojoWithMimeType.receivedPojos).hasSize(1);
			Assertions.assertThat(testPojoWithMimeType.receivedPojos.get(0))
					.hasFieldOrPropertyWithValue("foo", "barbar" + id);
			Message<String> message = (Message<String>) collector
					.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			assertThat(new String(message.getPayload()))
					.isEqualTo("{\"bar\":\"barbar" + id + "\"}");
			assertThat(
					message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
							.includes(MimeTypeUtils.APPLICATION_JSON));
			context.close();
		}

	}

	@RunWith(Parameterized.class)
	public static class TestReturnNoConversion {

		private Class<?> configClass;

		private ObjectMapper mapper = new ObjectMapper();

		public TestReturnNoConversion(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection<?> InputConfigs() {
			return Arrays.asList(new Class[] { TestPojoWithMimeType1.class,
					TestPojoWithMimeType2.class });
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testReturnNoConversion() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(
					this.configClass, "--server.port=0", "--spring.jmx.enabled=false");
			MessageCollector collector = context.getBean(MessageCollector.class);
			Processor processor = context.getBean(Processor.class);
			String id = UUID.randomUUID().toString();
			processor.input()
					.send(MessageBuilder.withPayload("{\"foo\":\"barbar" + id + "\"}")
							.setHeader("contentType", "application/json").build());
			TestPojoWithMimeType testPojoWithMimeType = context
					.getBean(TestPojoWithMimeType.class);
			Assertions.assertThat(testPojoWithMimeType.receivedPojos).hasSize(1);
			Assertions.assertThat(testPojoWithMimeType.receivedPojos.get(0))
					.hasFieldOrPropertyWithValue("foo", "barbar" + id);
			Message<String> message = (Message<String>) collector
					.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			StreamListenerTestUtils.BarPojo barPojo = this.mapper.readValue(
					message.getPayload(), StreamListenerTestUtils.BarPojo.class);
			assertThat(barPojo.getBar()).isEqualTo("barbar" + id);
			assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE,
					MimeType.class) != null);
			context.close();
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMimeType1 extends TestPojoWithMimeType {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public StreamListenerTestUtils.BarPojo receive(
				StreamListenerTestUtils.FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			StreamListenerTestUtils.BarPojo barPojo = new StreamListenerTestUtils.BarPojo();
			barPojo.setBar(fooPojo.getFoo());
			return barPojo;
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMimeType2 extends TestPojoWithMimeType {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public StreamListenerTestUtils.BarPojo receive(
				StreamListenerTestUtils.FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			StreamListenerTestUtils.BarPojo barPojo = new StreamListenerTestUtils.BarPojo();
			barPojo.setBar(fooPojo.getFoo());
			return barPojo;
		}

	}

	public static class TestPojoWithMimeType {

		List<StreamListenerTestUtils.FooPojo> receivedPojos = new ArrayList<>();

	}

}
