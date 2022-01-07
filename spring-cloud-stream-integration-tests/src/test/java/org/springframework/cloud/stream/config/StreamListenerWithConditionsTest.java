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
import java.util.List;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Marius Bogoevici
 */
public class StreamListenerWithConditionsTest {

	@Test
	@Ignore
	public void testAnnotatedArgumentsWithConditionalClass() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestPojoWithAnnotatedArguments.class, "--server.port=0");

		TestPojoWithAnnotatedArguments testPojoWithAnnotatedArguments = context
				.getBean(TestPojoWithAnnotatedArguments.class);
		Sink sink = context.getBean(Sink.class);
		String id = UUID.randomUUID().toString();
		sink.input().send(MessageBuilder.withPayload("{\"foo\":\"barbar" + id + "\"}")
				.setHeader("contentType", "application/json")
				.setHeader("testHeader", "testValue").setHeader("type", "foo").build());
		sink.input().send(MessageBuilder.withPayload("{\"bar\":\"foofoo" + id + "\"}")
				.setHeader("contentType", "application/json")
				.setHeader("testHeader", "testValue").setHeader("type", "bar").build());
		sink.input().send(MessageBuilder.withPayload("{\"bar\":\"foofoo" + id + "\"}")
				.setHeader("contentType", "application/json")
				.setHeader("testHeader", "testValue").setHeader("type", "qux").build());
		assertThat(testPojoWithAnnotatedArguments.receivedFoo).hasSize(1);
		assertThat(testPojoWithAnnotatedArguments.receivedFoo.get(0))
				.hasFieldOrPropertyWithValue("foo", "barbar" + id);
		assertThat(testPojoWithAnnotatedArguments.receivedBar).hasSize(1);
		assertThat(testPojoWithAnnotatedArguments.receivedBar.get(0))
				.hasFieldOrPropertyWithValue("bar", "foofoo" + id);
		context.close();
	}

	@Test
	public void testConditionalFailsWithReturnValue() throws Exception {
		try {
			ConfigurableApplicationContext context = SpringApplication.run(
					TestConditionalOnMethodWithReturnValueFails.class, "--server.port=0");
			context.close();
			fail("Context creation failure expected");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(
					StreamListenerErrorMessages.CONDITION_ON_METHOD_RETURNING_VALUE);
		}
	}

	@Test
	public void testConditionalFailsWithDeclarativeMethod() throws Exception {
		try {
			ConfigurableApplicationContext context = SpringApplication.run(
					TestConditionalOnDeclarativeMethodFails.class, "--server.port=0");
			context.close();
			fail("Context creation failure expected");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(
					StreamListenerErrorMessages.CONDITION_ON_DECLARATIVE_METHOD);
		}
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestPojoWithAnnotatedArguments {

		List<StreamListenerTestUtils.FooPojo> receivedFoo = new ArrayList<>();

		List<StreamListenerTestUtils.BarPojo> receivedBar = new ArrayList<>();

		@StreamListener(value = Sink.INPUT, condition = "headers['type']=='foo'")
		public void receive(@Payload StreamListenerTestUtils.FooPojo fooPojo) {
			this.receivedFoo.add(fooPojo);
		}

		@StreamListener(target = Sink.INPUT, condition = "headers['type']=='bar'")
		public void receive(@Payload StreamListenerTestUtils.BarPojo barPojo) {
			this.receivedBar.add(barPojo);
		}

	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestConditionalOnDeclarativeMethodFails {

		@StreamListener(condition = "headers['type']=='foo'")
		public void receive(@Input("input") MessageChannel input) {
			// do nothing
		}

	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestConditionalOnMethodWithReturnValueFails {

		@StreamListener(value = Sink.INPUT, condition = "headers['type']=='foo'")
		public String receive(String value) {
			return null;
		}

	}

}
