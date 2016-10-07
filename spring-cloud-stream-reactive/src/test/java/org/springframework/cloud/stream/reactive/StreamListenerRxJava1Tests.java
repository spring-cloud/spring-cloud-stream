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

package org.springframework.cloud.stream.reactive;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import rx.Observable;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@RunWith(StreamListenerRxJava1Tests.class)
@Suite.SuiteClasses({StreamListenerRxJava1Tests.TestInputOutputArgs.class, StreamListenerRxJava1Tests.TestInputOutputArgsWithMessage.class,
		StreamListenerRxJava1Tests.TestInputOutputArgsWithObservableSender.class, StreamListenerRxJava1Tests.TestReturn.class,
		StreamListenerRxJava1Tests.TestReturnWithFailure.class, StreamListenerRxJava1Tests.TestReturnWithMessage.class,
		StreamListenerRxJava1Tests.TestReturnWithMessage.class, StreamListenerRxJava1Tests.TestReturnWithPojo.class})
public class StreamListenerRxJava1Tests extends Suite {

	public StreamListenerRxJava1Tests(Class<?> klass, RunnerBuilder builder) throws InitializationError {
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
			return Arrays.asList(new Class[] {TestInputOutputArgs1.class, TestInputOutputArgs2.class});
		}

		@Test
		public void testInputOutputArgs() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			sendMessageAndValidate(context);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestInputOutputArgsWithMessage {

		private Class<?> configClass;

		public TestInputOutputArgsWithMessage(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestInputOutputArgsWithMessage1.class, TestInputOutputArgsWithMessage2.class});
		}

		@Test
		public void testInputOutputArgsWithMessage() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
					"--server.port=0");
			sendMessageAndValidate(context);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestInputOutputArgsWithObservableSender {

		private Class<?> configClass;

		public TestInputOutputArgsWithObservableSender(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestInputOutputArgsWithObservableSender1.class, TestInputOutputArgsWithObservableSender2.class});
		}

		@Test
		public void testInputOutputArgsWithObservableSender() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
					"--server.port=0");
			// send multiple message
			sendMessageAndValidate(context);
			sendMessageAndValidate(context);
			sendMessageAndValidate(context);
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
			return Arrays.asList(new Class[] {TestReturn1.class, TestReturn2.class, TestReturn3.class, TestReturn4.class});
		}

		@Test
		public void testReturn() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			sendMessageAndValidate(context);
			sendMessageAndValidate(context);
			sendMessageAndValidate(context);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestReturnWithFailure {

		private Class<?> configClass;

		public TestReturnWithFailure(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestReturnWithFailure1.class, TestReturnWithFailure2.class,
					TestReturnWithFailure3.class, TestReturnWithFailure4.class});
		}

		@Test
		public void testReturnWithFailure() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			sendMessageAndValidate(context);
			sendFailingMessage(context);
			sendMessageAndValidate(context);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestReturnWithMessage {

		private Class<?> configClass;

		public TestReturnWithMessage(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestReturnWithMessage1.class, TestReturnWithMessage2.class,
					TestReturnWithMessage3.class, TestReturnWithMessage4.class});
		}

		@Test
		public void testReturnWithMessage() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			sendMessageAndValidate(context);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestReturnWithPojo {

		private Class<?> configClass;

		public TestReturnWithPojo(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestReturnWithPojo1.class, TestReturnWithPojo2.class,
					TestReturnWithPojo3.class, TestReturnWithPojo4.class});
		}

		@Test
		public void testReturnWithPojo() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			@SuppressWarnings("unchecked")
			Processor processor = context.getBean(Processor.class);
			processor.input().send(MessageBuilder.withPayload("{\"message\":\"helloPojo\"}")
					.setHeader("contentType", "application/json").build());
			MessageCollector messageCollector = context.getBean(MessageCollector.class);
			Message<?> result = messageCollector.forChannel(processor.output()).poll(1000, TimeUnit.MILLISECONDS);
			assertThat(result).isNotNull();
			assertThat(result.getPayload()).isInstanceOf(BarPojo.class);
			assertThat(((BarPojo) result.getPayload()).getBarMessage()).isEqualTo("helloPojo");
			context.close();
		}
	}

	private static void sendMessageAndValidate(ConfigurableApplicationContext context) throws InterruptedException {
		@SuppressWarnings("unchecked")
		Processor processor = context.getBean(Processor.class);
		String sentPayload = "hello " + UUID.randomUUID().toString();
		processor.input().send(MessageBuilder.withPayload(sentPayload).setHeader("contentType", "text/plain").build());
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Message<?> result = messageCollector.forChannel(processor.output()).poll(1000, TimeUnit.MILLISECONDS);
		assertThat(result).isNotNull();
		assertThat(result.getPayload()).isEqualTo(sentPayload.toUpperCase());
	}

	private static void sendFailingMessage(ConfigurableApplicationContext context) throws InterruptedException {
		@SuppressWarnings("unchecked")
		Processor processor = context.getBean(Processor.class);
		processor.input().send(MessageBuilder.withPayload("fail").setHeader("contentType", "text/plain").build());
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgs1 {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Observable<String> input, @Output(Processor.OUTPUT) ObservableSender output) {
			output.send(input.map(m -> m.toUpperCase()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgs2 {

		@StreamListener(Processor.INPUT)
		public void receive(Observable<String> input, @Output(Processor.OUTPUT) ObservableSender output) {
			output.send(input.map(m -> m.toUpperCase()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithMessage1 {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Observable<Message<String>> input,
				@Output(Processor.OUTPUT) ObservableSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload(m.getPayload().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithMessage2 {

		@StreamListener(Processor.INPUT)
		public void receive(Observable<Message<String>> input,
				@Output(Processor.OUTPUT) ObservableSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload(m.getPayload().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithObservableSender1 {
		@StreamListener
		public void receive(@Input(Processor.INPUT) Observable<Message<?>> input, @Output(Processor.OUTPUT)
				ObservableSender output) {
			output.send(input
					.map(m -> m.getPayload().toString().toUpperCase())
					.map(o -> MessageBuilder.withPayload(o).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithObservableSender2 {
		@StreamListener(Processor.INPUT)
		public void receive(Observable<Message<?>> input, @Output(Processor.OUTPUT)
		ObservableSender output) {
			output.send(input
					.map(m -> m.getPayload().toString().toUpperCase())
					.map(o -> MessageBuilder.withPayload(o).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturn1 {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
		Observable<String> receive(@Input(Processor.INPUT) Observable<String> input) {
			return input.map(m -> m.toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturn2 {

		@StreamListener(Processor.INPUT)
		public
		@Output(Processor.OUTPUT)
		Observable<String> receive(Observable<String> input) {
			return input.map(m -> m.toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturn3 {

		@StreamListener(Processor.INPUT)
		public
		@SendTo(Processor.OUTPUT)
		Observable<String> receive(Observable<String> input) {
			return input.map(m -> m.toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturn4 {

		@StreamListener
		public
		@SendTo(Processor.OUTPUT)
		Observable<String> receive(@Input(Processor.INPUT) Observable<String> input) {
			return input.map(m -> m.toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithFailure1 {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
		Observable<String> receive(@Input(Processor.INPUT) Observable<String> input) {
			return input.map(m -> {
				if (!m.equals("fail")) {
					return m.toUpperCase();
				}
				else {
					throw new RuntimeException();
				}
			});
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithFailure2 {

		@StreamListener
		public
		@SendTo(Processor.OUTPUT)
		Observable<String> receive(@Input(Processor.INPUT) Observable<String> input) {
			return input.map(m -> {
				if (!m.equals("fail")) {
					return m.toUpperCase();
				}
				else {
					throw new RuntimeException();
				}
			});
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithFailure3 {

		@StreamListener(Processor.INPUT)
		public
		@SendTo(Processor.OUTPUT)
		Observable<String> receive(Observable<String> input) {
			return input.map(m -> {
				if (!m.equals("fail")) {
					return m.toUpperCase();
				}
				else {
					throw new RuntimeException();
				}
			});
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithFailure4 {

		@StreamListener(Processor.INPUT)
		public
		@Output(Processor.OUTPUT)
		Observable<String> receive(Observable<String> input) {
			return input.map(m -> {
				if (!m.equals("fail")) {
					return m.toUpperCase();
				}
				else {
					throw new RuntimeException();
				}
			});
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithMessage1 {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
		Observable<String> receive(@Input(Processor.INPUT) Observable<Message<String>> input) {
			return input.map(m -> m.getPayload().toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithMessage2 {

		@StreamListener
		public
		@SendTo(Processor.OUTPUT)
		Observable<String> receive(@Input(Processor.INPUT) Observable<Message<String>> input) {
			return input.map(m -> m.getPayload().toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithMessage3 {

		@StreamListener(Processor.INPUT)
		public
		@Output(Processor.OUTPUT)
		Observable<String> receive(Observable<Message<String>> input) {
			return input.map(m -> m.getPayload().toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithMessage4 {

		@StreamListener(Processor.INPUT)
		public
		@SendTo(Processor.OUTPUT)
		Observable<String> receive(Observable<Message<String>> input) {
			return input.map(m -> m.getPayload().toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithPojo1 {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
		Observable<BarPojo> receive(@Input(Processor.INPUT) Observable<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithPojo2 {

		@StreamListener
		public
		@SendTo(Processor.OUTPUT)
		Observable<BarPojo> receive(@Input(Processor.INPUT) Observable<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithPojo3 {

		@StreamListener(Processor.INPUT)
		public
		@Output(Processor.OUTPUT)
		Observable<BarPojo> receive(Observable<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithPojo4 {

		@StreamListener(Processor.INPUT)
		public
		@SendTo(Processor.OUTPUT)
		Observable<BarPojo> receive(Observable<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}
	}

	public static class FooPojo {

		private String message;

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}
	}

	public static class BarPojo {

		private String barMessage;

		public BarPojo(String barMessage) {
			this.barMessage = barMessage;
		}

		public String getBarMessage() {
			return barMessage;
		}

		public void setBarMessage(String barMessage) {
			this.barMessage = barMessage;
		}
	}
}
