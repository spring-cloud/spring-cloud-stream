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

package org.springframework.cloud.stream.config;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;

/**
 * @author Marius Bogoevici
 */
public class StreamListenerTests {

	@Test
	public void testContentTypeConversion() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestSink.class);
		@SuppressWarnings("unchecked")
		TestSink testSink = context.getBean(TestSink.class);
		Sink sink = context.getBean(Sink.class);
		String id = UUID.randomUUID().toString();
		sink.input().send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
				.setHeader("contentType", "application/json").build());
		assertTrue(testSink.latch.await(10, TimeUnit.SECONDS));
		assertThat(testSink.receivedArguments, hasSize(1));
		assertThat(testSink.receivedArguments.get(0),
				hasProperty("bar", equalTo("barbar" + id)));
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAnnotatedArguments() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestPojoWithAnnotatedArguments.class);

		TestPojoWithAnnotatedArguments testPojoWithAnnotatedArguments = context
				.getBean(TestPojoWithAnnotatedArguments.class);
		Sink sink = context.getBean(Sink.class);
		String id = UUID.randomUUID().toString();
		sink.input()
				.send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
						.setHeader("contentType", "application/json")
						.setHeader("testHeader", "testValue").build());
		assertThat(testPojoWithAnnotatedArguments.receivedArguments, hasSize(3));
		assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(0),
				instanceOf(FooPojo.class));
		assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(0),
				hasProperty("bar", equalTo("barbar" + id)));
		assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(1),
				instanceOf(Map.class));
		assertThat(
				(Map<String, String>) testPojoWithAnnotatedArguments.receivedArguments
						.get(1),
				hasEntry(MessageHeaders.CONTENT_TYPE, "application/json"));
		assertThat((Map<String, String>) testPojoWithAnnotatedArguments.receivedArguments
				.get(1), hasEntry(equalTo("testHeader"), equalTo("testValue")));
		assertThat((String) testPojoWithAnnotatedArguments.receivedArguments.get(2),
				equalTo("application/json"));
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReturn() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestStringProcessor.class);
		MessageCollector collector = context.getBean(MessageCollector.class);
		Processor processor = context.getBean(Processor.class);
		String id = UUID.randomUUID().toString();
		processor.input()
				.send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
						.setHeader("contentType", "application/json").build());
		Message<String> message = (Message<String>) collector
				.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
		TestStringProcessor testStringProcessor = context
				.getBean(TestStringProcessor.class);
		assertThat(testStringProcessor.receivedPojos, hasSize(1));
		assertThat(testStringProcessor.receivedPojos.get(0),
				hasProperty("bar", equalTo("barbar" + id)));
		assertThat(message, not(nullValue(Message.class)));
		assertThat(message.getPayload(), equalTo("barbar" + id));
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReturnConversion() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestPojoWithMimeType.class,
				"--spring.cloud.stream.bindings.output.contentType=application/json");
		MessageCollector collector = context.getBean(MessageCollector.class);
		Processor processor = context.getBean(Processor.class);
		String id = UUID.randomUUID().toString();
		processor.input()
				.send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
						.setHeader("contentType", "application/json").build());
		TestPojoWithMimeType testPojoWithMimeType = context
				.getBean(TestPojoWithMimeType.class);
		assertThat(testPojoWithMimeType.receivedPojos, hasSize(1));
		assertThat(testPojoWithMimeType.receivedPojos.get(0),
				hasProperty("bar", equalTo("barbar" + id)));
		Message<String> message = (Message<String>) collector
				.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message, not(nullValue(Message.class)));
		assertThat(message.getPayload(), equalTo("{\"qux\":\"barbar" + id + "\"}"));
		assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE, String.class),
				equalTo("application/json"));
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReturnNoConversion() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestPojoWithMimeType.class);
		MessageCollector collector = context.getBean(MessageCollector.class);
		Processor processor = context.getBean(Processor.class);
		String id = UUID.randomUUID().toString();
		processor.input()
				.send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
						.setHeader("contentType", "application/json").build());
		TestPojoWithMimeType testPojoWithMimeType = context
				.getBean(TestPojoWithMimeType.class);
		assertThat(testPojoWithMimeType.receivedPojos, hasSize(1));
		assertThat(testPojoWithMimeType.receivedPojos.get(0),
				hasProperty("bar", equalTo("barbar" + id)));
		Message<BazPojo> message = (Message<BazPojo>) collector
				.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message, not(nullValue(Message.class)));
		assertThat(message.getPayload().getQux(), equalTo("barbar" + id));
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReturnMessage() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestPojoWithMessageReturn.class);
		MessageCollector collector = context.getBean(MessageCollector.class);
		Processor processor = context.getBean(Processor.class);
		String id = UUID.randomUUID().toString();
		processor.input()
				.send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
						.setHeader("contentType", "application/json").build());
		TestPojoWithMessageReturn testPojoWithMessageReturn = context
				.getBean(TestPojoWithMessageReturn.class);
		assertThat(testPojoWithMessageReturn.receivedPojos, hasSize(1));
		assertThat(testPojoWithMessageReturn.receivedPojos.get(0),
				hasProperty("bar", equalTo("barbar" + id)));
		Message<BazPojo> message = (Message<BazPojo>) collector
				.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message, not(nullValue(Message.class)));
		assertThat(message.getPayload().getQux(), equalTo("barbar" + id));
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMessageArgument() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestPojoWithMessageArgument.class);
		MessageCollector collector = context.getBean(MessageCollector.class);
		Processor processor = context.getBean(Processor.class);
		String id = UUID.randomUUID().toString();
		processor.input().send(MessageBuilder.withPayload("barbar" + id)
				.setHeader("contentType", "text/plain").build());
		TestPojoWithMessageArgument testPojoWithMessageArgument = context
				.getBean(TestPojoWithMessageArgument.class);
		assertThat(testPojoWithMessageArgument.receivedMessages, hasSize(1));
		assertThat(testPojoWithMessageArgument.receivedMessages.get(0).getPayload(),
				equalTo("barbar" + id));
		Message<BazPojo> message = (Message<BazPojo>) collector
				.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message, not(nullValue(Message.class)));
		assertThat(message.getPayload().getQux(), equalTo("barbar" + id));
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDuplicateMapping() throws Exception {
		try {
			ConfigurableApplicationContext context = SpringApplication
					.run(TestDuplicateMapping.class);
			fail("Exception expected on duplicate mapping");
		}
		catch (BeanCreationException e) {
			assertThat(e.getCause().getMessage(),
					startsWith("Duplicate @StreamListener mapping"));
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testHandlerBean() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestHandlerBean.class,
				"--spring.cloud.stream.bindings.output.contentType=application/json");
		MessageCollector collector = context.getBean(MessageCollector.class);
		Processor processor = context.getBean(Processor.class);
		String id = UUID.randomUUID().toString();
		processor.input()
				.send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
						.setHeader("contentType", "application/json").build());
		HandlerBean handlerBean = context.getBean(HandlerBean.class);
		assertThat(handlerBean.receivedPojos, hasSize(1));
		assertThat(handlerBean.receivedPojos.get(0),
				hasProperty("bar", equalTo("barbar" + id)));
		Message<String> message = (Message<String>) collector
				.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message, not(nullValue(Message.class)));
		assertThat(message.getPayload(), equalTo("{\"qux\":\"barbar" + id + "\"}"));
		assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE, String.class),
				equalTo("application/json"));
		context.close();
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestSink {

		List<FooPojo> receivedArguments = new ArrayList<>();

		CountDownLatch latch = new CountDownLatch(1);

		@StreamListener(Sink.INPUT)
		public void receive(FooPojo fooPojo) {
			receivedArguments.add(fooPojo);
			latch.countDown();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestStringProcessor {

		List<FooPojo> receivedPojos = new ArrayList<>();

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String receive(FooPojo fooPojo) {
			receivedPojos.add(fooPojo);
			return fooPojo.getBar();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMimeType {

		List<FooPojo> receivedPojos = new ArrayList<>();

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(FooPojo fooPojo) {
			receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return bazPojo;
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithAnnotatedArguments {

		List<Object> receivedArguments = new ArrayList<>();

		@StreamListener(Processor.INPUT)
		public void receive(@Payload FooPojo fooPojo,
				@Headers Map<String, Object> headers,
				@Header(MessageHeaders.CONTENT_TYPE) String contentType) {
			receivedArguments.add(fooPojo);
			receivedArguments.add(headers);
			receivedArguments.add(contentType);
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageReturn {

		List<FooPojo> receivedPojos = new ArrayList<>();

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Message<?> receive(FooPojo fooPojo) {
			receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return MessageBuilder.withPayload(bazPojo).setHeader("foo", "bar").build();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageArgument {

		List<Message<String>> receivedMessages = new ArrayList<>();

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(Message<String> fooMessage) {
			receivedMessages.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getPayload());
			return bazPojo;
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestDuplicateMapping {

		@StreamListener(Processor.INPUT)
		public void receive(Message<String> fooMessage) {
		}

		@StreamListener(Processor.INPUT)
		public void receive2(Message<String> fooMessage) {
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean {

		@Bean
		public HandlerBean handlerBean() {
			return new HandlerBean();
		}
	}

	public static class HandlerBean {

		List<FooPojo> receivedPojos = new ArrayList<>();

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(FooPojo fooMessage) {
			receivedPojos.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getBar());
			return bazPojo;
		}
	}

	public static class FooPojo {

		private String bar;

		public String getBar() {
			return bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}
	}

	public static class BazPojo {

		private String qux;

		public String getQux() {
			return qux;
		}

		public void setQux(String qux) {
			this.qux = qux;
		}
	}
}
