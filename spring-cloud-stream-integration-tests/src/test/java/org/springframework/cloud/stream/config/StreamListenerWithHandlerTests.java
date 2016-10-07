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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@RunWith(StreamListenerWithHandlerTests.class)
@Suite.SuiteClasses({ StreamListenerWithHandlerTests.TestInputOutputArgs.class, StreamListenerWithHandlerTests.TestAnnotatedArguments.class,
		StreamListenerWithHandlerTests.TestReturn.class, StreamListenerWithHandlerTests.TestReturnConversion.class,
		StreamListenerWithHandlerTests.TestReturnNoConversion.class, StreamListenerWithHandlerTests.TestReturnMessage.class,
		StreamListenerWithHandlerTests.TestMessageArgument.class, StreamListenerWithHandlerTests.TestDuplicateMapping.class,
		StreamListenerWithHandlerTests.TestHandlerBean.class, StreamListenerWithHandlerTests.TestStreamListenerMethod.class })
public class StreamListenerWithHandlerTests extends Suite {

	public StreamListenerWithHandlerTests(Class<?> klass, RunnerBuilder builder) throws InitializationError {
		super(klass, builder);
	}

	@RunWith(Parameterized.class)
	public static class TestInputOutputArgs {

		private Class<?> configClass;

		public TestInputOutputArgs(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestSink1.class, TestSink2.class});
		}

		@Test
		public void testContentTypeConversion() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			@SuppressWarnings("unchecked")
			TestSink testSink = context.getBean(TestSink.class);
			Sink sink = context.getBean(Sink.class);
			String id = UUID.randomUUID().toString();
			sink.input().send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
					.setHeader("contentType", "application/json").build());
			assertThat(testSink.latch.await(10, TimeUnit.SECONDS));
			assertThat(testSink.receivedArguments).hasSize(1);
			assertThat(testSink.receivedArguments.get(0)).hasFieldOrPropertyWithValue("bar", "barbar" + id);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestAnnotatedArguments {

		private Class<?> configClass;

		public TestAnnotatedArguments(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestPojoWithAnnotatedArguments1.class, TestPojoWithAnnotatedArguments2.class});
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testAnnotatedArguments() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
					"--server.port=0");

			TestPojoWithAnnotatedArguments testPojoWithAnnotatedArguments = context
					.getBean(TestPojoWithAnnotatedArguments.class);
			Sink sink = context.getBean(Sink.class);
			String id = UUID.randomUUID().toString();
			sink.input().send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
					.setHeader("contentType", "application/json").setHeader("testHeader", "testValue").build());
			assertThat(testPojoWithAnnotatedArguments.receivedArguments).hasSize(3);
			assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(0)).isInstanceOf(FooPojo.class);
			assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(0)).hasFieldOrPropertyWithValue("bar",
					"barbar" + id);
			assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(1)).isInstanceOf(Map.class);
			assertThat((Map<String, String>) testPojoWithAnnotatedArguments.receivedArguments.get(1))
					.containsEntry(MessageHeaders.CONTENT_TYPE, "application/json");
			assertThat((Map<String, String>) testPojoWithAnnotatedArguments.receivedArguments.get(1))
					.containsEntry("testHeader", "testValue");
			assertThat(testPojoWithAnnotatedArguments.receivedArguments.get(2)).isEqualTo("application/json");
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestReturn {

		private Class<?> configClass;

		public TestReturn(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestStringProcessor1.class, TestStringProcessor2.class,
					TestStringProcessor3.class, TestStringProcessor4.class});
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testReturn() throws Exception {
			ConfigurableApplicationContext context = SpringApplication
					.run(this.configClass, "--server.port=0");
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
			assertThat(testStringProcessor.receivedPojos).hasSize(1);
			assertThat(testStringProcessor.receivedPojos.get(0)).hasFieldOrPropertyWithValue("bar", "barbar" + id);
			assertThat(message).isNotNull();
			assertThat(message.getPayload()).isEqualTo("barbar" + id);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestReturnConversion {

		private Class<?> configClass;

		public TestReturnConversion(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestPojoWithMimeType1.class, TestPojoWithMimeType2.class,
					TestPojoWithMimeType3.class, TestPojoWithMimeType4.class});
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testReturnConversion() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
					"--spring.cloud.stream.bindings.output.contentType=application/json", "--server.port=0");
			MessageCollector collector = context.getBean(MessageCollector.class);
			Processor processor = context.getBean(Processor.class);
			String id = UUID.randomUUID().toString();
			processor.input().send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
					.setHeader("contentType", "application/json").build());
			TestPojoWithMimeType testPojoWithMimeType = context.getBean(TestPojoWithMimeType.class);
			assertThat(testPojoWithMimeType.receivedPojos).hasSize(1);
			assertThat(testPojoWithMimeType.receivedPojos.get(0)).hasFieldOrPropertyWithValue("bar", "barbar" + id);
			Message<String> message = (Message<String>) collector.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			assertThat(message.getPayload()).isEqualTo("{\"qux\":\"barbar" + id + "\"}");
			assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class).includes(MimeTypeUtils.APPLICATION_JSON));
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestReturnNoConversion {

		private Class<?> configClass;

		public TestReturnNoConversion(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestPojoWithMimeType1.class, TestPojoWithMimeType2.class,
					TestPojoWithMimeType3.class, TestPojoWithMimeType4.class});
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testReturnNoConversion() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			MessageCollector collector = context.getBean(MessageCollector.class);
			Processor processor = context.getBean(Processor.class);
			String id = UUID.randomUUID().toString();
			processor.input().send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
					.setHeader("contentType", "application/json").build());
			TestPojoWithMimeType testPojoWithMimeType = context.getBean(TestPojoWithMimeType.class);
			assertThat(testPojoWithMimeType.receivedPojos).hasSize(1);
			assertThat(testPojoWithMimeType.receivedPojos.get(0)).hasFieldOrPropertyWithValue("bar", "barbar" + id);
			Message<BazPojo> message = (Message<BazPojo>) collector.forChannel(processor.output()).poll(1,
					TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			assertThat(message.getPayload().getQux()).isEqualTo("barbar" + id);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestReturnMessage {

		private Class<?> configClass;

		public TestReturnMessage(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestPojoWithMessageReturn1.class, TestPojoWithMessageReturn2.class,
					TestPojoWithMessageReturn3.class, TestPojoWithMessageReturn4.class});
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testReturnMessage() throws Exception {
			ConfigurableApplicationContext context = SpringApplication
					.run(this.configClass, "--server.port=0");
			MessageCollector collector = context.getBean(MessageCollector.class);
			Processor processor = context.getBean(Processor.class);
			String id = UUID.randomUUID().toString();
			processor.input()
					.send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
							.setHeader("contentType", "application/json").build());
			TestPojoWithMessageReturn testPojoWithMessageReturn = context
					.getBean(TestPojoWithMessageReturn.class);
			assertThat(testPojoWithMessageReturn.receivedPojos).hasSize(1);
			assertThat(testPojoWithMessageReturn.receivedPojos.get(0)).hasFieldOrPropertyWithValue("bar", "barbar" + id);
			Message<BazPojo> message = (Message<BazPojo>) collector
					.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			assertThat(message.getPayload().getQux()).isEqualTo("barbar" + id);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestMessageArgument {

		private Class<?> configClass;

		public TestMessageArgument(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestPojoWithMessageArgument1.class, TestPojoWithMessageArgument2.class,
					TestPojoWithMessageArgument3.class, TestPojoWithMessageArgument4.class});
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testMessageArgument() throws Exception {
			ConfigurableApplicationContext context = SpringApplication
					.run(this.configClass, "--server.port=0");
			MessageCollector collector = context.getBean(MessageCollector.class);
			Processor processor = context.getBean(Processor.class);
			String id = UUID.randomUUID().toString();
			processor.input().send(MessageBuilder.withPayload("barbar" + id)
					.setHeader("contentType", "text/plain").build());
			TestPojoWithMessageArgument testPojoWithMessageArgument = context
					.getBean(TestPojoWithMessageArgument.class);
			assertThat(testPojoWithMessageArgument.receivedMessages).hasSize(1);
			assertThat(testPojoWithMessageArgument.receivedMessages.get(0).getPayload()).isEqualTo("barbar" + id);
			Message<BazPojo> message = (Message<BazPojo>) collector
					.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			assertThat(message.getPayload().getQux()).isEqualTo("barbar" + id);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestDuplicateMapping {

		private Class<?> configClass;

		public TestDuplicateMapping(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestDuplicateMapping1.class, TestDuplicateMapping2.class,
					TestDuplicateMapping3.class, TestDuplicateMapping4.class});
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testDuplicateMapping() throws Exception {
			try {
				ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
						"--server.port=0");
				fail("Exception expected on duplicate mapping");
			}
			catch (BeanCreationException e) {
				assertThat(e.getCause().getMessage()).startsWith("Duplicate @StreamListener mapping");
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class TestHandlerBean {

		private Class<?> configClass;

		public TestHandlerBean(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestHandlerBean1.class, TestHandlerBean2.class,
					TestHandlerBean3.class, TestHandlerBean4.class});
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testHandlerBean() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
					"--spring.cloud.stream.bindings.output.contentType=application/json", "--server.port=0");
			MessageCollector collector = context.getBean(MessageCollector.class);
			Processor processor = context.getBean(Processor.class);
			String id = UUID.randomUUID().toString();
			processor.input().send(MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
					.setHeader("contentType", "application/json").build());
			HandlerBean handlerBean = context.getBean(HandlerBean.class);
			assertThat(handlerBean.receivedPojos).hasSize(1);
			assertThat(handlerBean.receivedPojos.get(0)).hasFieldOrPropertyWithValue("bar", "barbar" + id);
			Message<String> message = (Message<String>) collector.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
			assertThat(message).isNotNull();
			assertThat(message.getPayload()).isEqualTo("{\"qux\":\"barbar" + id + "\"}");
			assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class).includes(MimeTypeUtils.APPLICATION_JSON));
			context.close();
		}
	}

	public static class TestStreamListenerMethod {

		@Test
		public void testMethodWithInputAsMethodAndParameter() throws Exception {
			try {
				SpringApplication.run(TestMethodWithInputAsMethodAndParameter.class, "--server.port=0");
				fail("Exception expected on using Input element as method and parameter");
			}
			catch (BeanCreationException e) {
				assertThat(e.getCause().getMessage()).startsWith("Cannot set both StreamListener value ");
			}
		}

		@Test
		public void testMethodWithOutputAsMethodAndParameter() throws Exception {
			try {
				SpringApplication.run(TestMethodWithOutputAsMethodAndParameter.class, "--server.port=0");
				fail("Exception expected on using Input element as method and parameter");
			}
			catch (BeanCreationException e) {
				assertThat(e.getCause().getMessage()).startsWith("Cannot set both Output (@Output/@SendTo) method annotation value ");
			}
		}

		@Test
		public void testMethodWithoutInput() throws Exception {
			try {
				SpringApplication.run(TestMethodWithoutInput.class, "--server.port=0");
				fail("Exception expected when inbound target is not set");
			}
			catch (BeanCreationException e) {
				assertThat(e.getCause().getMessage()).startsWith("Either StreamListener or a method parameter should be set with ");
			}
		}

		@Test
		public void testMethodWithMultipleInputParameters() throws Exception {
			try {
				SpringApplication.run(TestMethodWithMultipleInputParameters.class, "--server.port=0");
				fail("Exception expected on multiple @Input method parameters");
			}
			catch (BeanCreationException e) {
				assertThat(e.getCause().getMessage()).startsWith("@Input annotation can not be used in multiple method parameters");
			}
		}

		@Test
		public void testMethodWithMultipleOutputParameters() throws Exception {
			try {
				SpringApplication.run(TestMethodWithMultipleOutputParameters.class, "--server.port=0");
				fail("Exception expected on multiple @Output method parameters");
			}
			catch (BeanCreationException e) {
				assertThat(e.getCause().getMessage()).startsWith("@Output annotation can not be used in multiple method parameters");
			}
		}
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestSink1 extends TestSink {

		@StreamListener(Sink.INPUT)
		public void receive(FooPojo fooPojo) {
			this.receivedArguments.add(fooPojo);
			this.latch.countDown();
		}
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestSink2 extends TestSink {

		@StreamListener
		public void receive(@Input(Processor.INPUT) FooPojo fooPojo) {
			this.receivedArguments.add(fooPojo);
			this.latch.countDown();
		}
	}

	public static class TestSink {
		List<FooPojo> receivedArguments = new ArrayList<>();
		CountDownLatch latch = new CountDownLatch(1);
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithAnnotatedArguments1 extends TestPojoWithAnnotatedArguments {

		@StreamListener(Processor.INPUT)
		public void receive(@Payload FooPojo fooPojo,
				@Headers Map<String, Object> headers,
				@Header(MessageHeaders.CONTENT_TYPE) String contentType) {
			this.receivedArguments.add(fooPojo);
			this.receivedArguments.add(headers);
			this.receivedArguments.add(contentType);
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithAnnotatedArguments2 extends TestPojoWithAnnotatedArguments {

		@StreamListener
		public void receive(@Input(Processor.INPUT) @Payload FooPojo fooPojo,
				@Headers Map<String, Object> headers,
				@Header(MessageHeaders.CONTENT_TYPE) String contentType) {
			this.receivedArguments.add(fooPojo);
			this.receivedArguments.add(headers);
			this.receivedArguments.add(contentType);
		}
	}

	public static class TestPojoWithAnnotatedArguments {
		List<Object> receivedArguments = new ArrayList<>();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestStringProcessor1 extends TestStringProcessor {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String receive(FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			return fooPojo.getBar();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestStringProcessor2 extends TestStringProcessor {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public String receive(FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			return fooPojo.getBar();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestStringProcessor3 extends TestStringProcessor {

		@StreamListener
		@SendTo(Processor.OUTPUT)
		public String receive(@Input(Processor.INPUT) FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			return fooPojo.getBar();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestStringProcessor4 extends TestStringProcessor {

		@StreamListener
		@Output(Processor.OUTPUT)
		public String receive(@Input(Processor.INPUT) FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			return fooPojo.getBar();
		}
	}

	public static class TestStringProcessor {

		List<FooPojo> receivedPojos = new ArrayList<>();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMimeType1 extends TestPojoWithMimeType {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return bazPojo;
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMimeType2 extends TestPojoWithMimeType {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public BazPojo receive(FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return bazPojo;
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMimeType3 extends TestPojoWithMimeType {

		@StreamListener
		@Output(Processor.OUTPUT)
		public BazPojo receive(@Input(Processor.INPUT) FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return bazPojo;
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMimeType4 extends TestPojoWithMimeType {

		@StreamListener
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(@Input(Processor.INPUT) FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return bazPojo;
		}
	}

	public static class TestPojoWithMimeType {

		List<FooPojo> receivedPojos = new ArrayList<>();

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageReturn1 extends TestPojoWithMessageReturn {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Message<?> receive(FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return MessageBuilder.withPayload(bazPojo).setHeader("foo", "bar").build();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageReturn2 extends TestPojoWithMessageReturn {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public Message<?> receive(FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return MessageBuilder.withPayload(bazPojo).setHeader("foo", "bar").build();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageReturn3 extends TestPojoWithMessageReturn {

		@StreamListener
		@Output(Processor.OUTPUT)
		public Message<?> receive(@Input(Processor.INPUT) FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return MessageBuilder.withPayload(bazPojo).setHeader("foo", "bar").build();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageReturn4 extends TestPojoWithMessageReturn {

		@StreamListener
		@SendTo(Processor.OUTPUT)
		public Message<?> receive(@Input(Processor.INPUT) FooPojo fooPojo) {
			this.receivedPojos.add(fooPojo);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooPojo.getBar());
			return MessageBuilder.withPayload(bazPojo).setHeader("foo", "bar").build();
		}
	}

	public static class TestPojoWithMessageReturn {

		List<FooPojo> receivedPojos = new ArrayList<>();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageArgument1 extends TestPojoWithMessageArgument {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(Message<String> fooMessage) {
			this.receivedMessages.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getPayload());
			return bazPojo;
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageArgument2 extends TestPojoWithMessageArgument {

		@StreamListener
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(@Input(Processor.INPUT) Message<String> fooMessage) {
			this.receivedMessages.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getPayload());
			return bazPojo;
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageArgument3 extends TestPojoWithMessageArgument {

		@StreamListener
		@Output(Processor.OUTPUT)
		public BazPojo receive(@Input(Processor.INPUT) Message<String> fooMessage) {
			this.receivedMessages.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getPayload());
			return bazPojo;
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageArgument4 extends TestPojoWithMessageArgument {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public BazPojo receive(Message<String> fooMessage) {
			this.receivedMessages.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getPayload());
			return bazPojo;
		}
	}

	public static class TestPojoWithMessageArgument {

		List<Message<String>> receivedMessages = new ArrayList<>();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestDuplicateMapping1 {

		@StreamListener(Processor.INPUT)
		public void receive(Message<String> fooMessage) {
		}

		@StreamListener(Processor.INPUT)
		public void receive2(Message<String> fooMessage) {
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestDuplicateMapping2 {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Message<String> fooMessage) {
		}

		@StreamListener(Processor.INPUT)
		public void receive2(Message<String> fooMessage) {
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestDuplicateMapping3 {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Message<String> fooMessage) {
		}

		@StreamListener
		public void receive2(@Input(Processor.INPUT) Message<String> fooMessage) {
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestDuplicateMapping4 {

		@StreamListener(Processor.INPUT)
		public void receive(Message<String> fooMessage) {
		}

		@StreamListener
		public void receive2(@Input(Processor.INPUT) Message<String> fooMessage) {
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean1 {

		@Bean
		public HandlerBean1 handlerBean() {
			return new HandlerBean1();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean2 {

		@Bean
		public HandlerBean2 handlerBean() {
			return new HandlerBean2();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean3 {

		@Bean
		public HandlerBean3 handlerBean() {
			return new HandlerBean3();
		}
	}


	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean4 {

		@Bean
		public HandlerBean4 handlerBean() {
			return new HandlerBean4();
		}
	}


	public static class HandlerBean1 extends HandlerBean {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getBar());
			return bazPojo;
		}
	}

	public static class HandlerBean2 extends HandlerBean {

		@StreamListener
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(@Input(Processor.INPUT) FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getBar());
			return bazPojo;
		}
	}

	public static class HandlerBean3 extends HandlerBean {

		@StreamListener
		@Output(Processor.OUTPUT)
		public BazPojo receive(@Input(Processor.INPUT) FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getBar());
			return bazPojo;
		}
	}

	public static class HandlerBean4 extends HandlerBean {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public BazPojo receive(FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getBar());
			return bazPojo;
		}
	}


	public static class HandlerBean {

		List<FooPojo> receivedPojos = new ArrayList<>();

	}

	public static class FooPojo {

		private String bar;

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}
	}

	public static class BazPojo {

		private String qux;

		public String getQux() {
			return this.qux;
		}

		public void setQux(String qux) {
			this.qux = qux;
		}
	}

	@EnableBinding({Processor.class, AnotherOutput.class})
	@EnableAutoConfiguration
	public static class TestMethodWithMultipleOutputParameters {

		@StreamListener
		public void receive(@Input(Processor.INPUT) SubscribableChannel input, @Output(Processor.OUTPUT) final MessageChannel output1,
				@Output(AnotherOutput.OUTPUT) final MessageChannel output2) {
			input.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					output1.send(org.springframework.messaging.support.MessageBuilder.withPayload(message.getPayload().toString().toUpperCase()).build());
				}
			});
		}
	}

	@EnableBinding({Sink.class})
	@EnableAutoConfiguration
	public static class TestMethodWithoutInput {

		@StreamListener
		public void receive(FooPojo fooPojo) {
		}
	}

	@EnableBinding({Sink.class})
	@EnableAutoConfiguration
	public static class TestMethodWithInputAsMethodAndParameter {

		@StreamListener(Processor.INPUT)
		public void receive(@Input(Processor.INPUT) FooPojo fooPojo, @Input(AnotherSink.INPUT) FooPojo fooPojo1) {
		}
	}

	@EnableBinding({Processor.class, AnotherOutput.class})
	@EnableAutoConfiguration
	public static class TestMethodWithOutputAsMethodAndParameter {

		@StreamListener
		@Output(AnotherOutput.OUTPUT)
		public void receive(@Input(Processor.INPUT) SubscribableChannel input, @Output(Processor.OUTPUT) final MessageChannel output1) {
			input.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					output1.send(org.springframework.messaging.support.MessageBuilder.withPayload(message.getPayload().toString().toUpperCase()).build());
				}
			});
		}
	}

	@EnableBinding({Sink.class, AnotherSink.class})
	@EnableAutoConfiguration
	public static class TestMethodWithMultipleInputParameters {

		@StreamListener
		public void receive(@Input(Processor.INPUT) FooPojo fooPojo, @Input(AnotherSink.INPUT) FooPojo fooPojo1) {
		}
	}

	public interface AnotherSink {

		String INPUT = "log";

		@Input(AnotherSink.INPUT)
		SubscribableChannel input();

	}

	public interface AnotherOutput {

		String OUTPUT = "test";

		@Output(AnotherOutput.OUTPUT)
		MessageChannel output();

	}
}
