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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
public class StreamListenerAnnotatedMethodArgumentsTests {

	@Test
	@SuppressWarnings("unchecked")
	public void testAnnotatedArguments() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestPojoWithAnnotatedArguments.class,
				"--server.port=0");

		TestPojoWithAnnotatedArguments testPojoWithAnnotatedArguments = context
				.getBean(TestPojoWithAnnotatedArguments.class);
		Sink sink = context.getBean(Sink.class);
		String id = UUID.randomUUID().toString();
		sink.input().send(MessageBuilder.withPayload("{\"foo\":\"barbar" + id + "\"}")
				.setHeader("contentType", MimeType.valueOf("application/json")).setHeader("testHeader", "testValue").build());
		assertThat(testPojoWithAnnotatedArguments.receivedArguments).hasSize(3);
		assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(0))
				.isInstanceOf(StreamListenerTestUtils.FooPojo.class);
		assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(0)).hasFieldOrPropertyWithValue("foo",
				"barbar" + id);
		assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(1)).isInstanceOf(Map.class);
		assertThat((Map<String, Object>) testPojoWithAnnotatedArguments.receivedArguments.get(1))
				.containsEntry(MessageHeaders.CONTENT_TYPE, MimeType.valueOf("application/json"));
		assertThat((Map<String, String>) testPojoWithAnnotatedArguments.receivedArguments.get(1))
				.containsEntry("testHeader", "testValue");
		assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(2)).isEqualTo("application/json");
		context.close();
	}

	@Test
	public void testInputAnnotationAtMethodParameter() throws Exception {
		try {
			SpringApplication.run(TestPojoWithInvalidInputAnnotatedArgument.class, "--server.port=0");
			fail("Exception expected: " + INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithAnnotatedArguments {

		List<Object> receivedArguments = new ArrayList<>();

		@StreamListener(Processor.INPUT)
		public void receive(@Payload StreamListenerTestUtils.FooPojo fooPojo,
				@Headers Map<String, Object> headers,
				@Header(MessageHeaders.CONTENT_TYPE) String contentType) {
			this.receivedArguments.add(fooPojo);
			this.receivedArguments.add(headers);
			this.receivedArguments.add(contentType);
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithInvalidInputAnnotatedArgument {

		List<Object> receivedArguments = new ArrayList<>();

		@StreamListener
		public void receive(
				@Input(Processor.INPUT) @Payload StreamListenerTestUtils.FooPojo fooPojo,
				@Headers Map<String, Object> headers,
				@Header(MessageHeaders.CONTENT_TYPE) String contentType) {
			this.receivedArguments.add(fooPojo);
			this.receivedArguments.add(headers);
			this.receivedArguments.add(contentType);
		}
	}
}
