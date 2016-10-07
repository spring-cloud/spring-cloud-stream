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
import reactor.core.publisher.Flux;

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
@RunWith(StreamListenerReactorTests.class)
@Suite.SuiteClasses({StreamListenerReactorTests.TestInputOutputArgs.class, StreamListenerReactorTests.TestInputOutputArgsWithMessage.class,
		StreamListenerReactorTests.TestInputOutputArgsWithFluxSender.class,
		StreamListenerReactorTests.TestInputOutputArgsWithFluxSenderAndFailure.class, StreamListenerReactorTests.TestReturn.class,
		StreamListenerReactorTests.TestReturnWithFailure.class, StreamListenerReactorTests.TestReturnWithMessage.class,
		StreamListenerReactorTests.TestReturnWithMessage.class, StreamListenerReactorTests.TestReturnWithPojo.class})
public class StreamListenerReactorTests extends Suite {

	public StreamListenerReactorTests(Class<?> klass, RunnerBuilder builder) throws InitializationError {
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
		public void testInputOutputArgs() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			sendMessageAndValidate(context);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestInputOutputArgsWithFluxSender {

		private Class<?> configClass;

		public TestInputOutputArgsWithFluxSender(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestInputOutputArgsWithFluxSender1.class, TestInputOutputArgsWithFluxSender2.class});
		}

		@Test
		public void testInputOutputArgsWithFluxSender() throws Exception {
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
	public static class TestInputOutputArgsWithFluxSenderAndFailure {

		private Class<?> configClass;

		public TestInputOutputArgsWithFluxSenderAndFailure(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestInputOutputArgsWithFluxSenderAndFailure1.class, TestInputOutputArgsWithFluxSenderAndFailure2.class});
		}

		@Test
		public void testInputOutputArgsWithFluxSenderAndFailure() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			sendMessageAndValidate(context);
			sendFailingMessage(context);
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

	@RunWith(Parameterized.class)
	public static class TestWildCardFluxInputOutputArgsWithMessage {

		private Class<?> configClass;

		public TestWildCardFluxInputOutputArgsWithMessage(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestWildCardFluxInputOutputArgsWithMessage1.class, TestWildCardFluxInputOutputArgsWithMessage2.class,
					TestWildCardFluxInputOutputArgsWithMessage3.class, TestWildCardFluxInputOutputArgsWithMessage4.class});
		}

		@Test
		public void testWildCardFluxInputOutputArgsWithMessage() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			sendMessageAndValidate(context);
			context.close();
		}
	}

	@RunWith(Parameterized.class)
	public static class TestGenericFluxInputOutputArgsWithMessage {

		private Class<?> configClass;

		public TestGenericFluxInputOutputArgsWithMessage(Class<?> configClass) {
			this.configClass = configClass;
		}

		@Parameterized.Parameters
		public static Collection InputConfigs() {
			return Arrays.asList(new Class[] {TestGenericStringFluxInputOutputArgsWithMessageImpl1.class, TestGenericStringFluxInputOutputArgsWithMessageImpl2.class,
					TestGenericStringFluxInputOutputArgsWithMessageImpl3.class, TestGenericStringFluxInputOutputArgsWithMessageImpl4.class});
		}

		@Test
		public void testGenericFluxInputOutputArgsWithMessage() throws Exception {
			ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
			sendMessageAndValidate(context);
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
		public void receive(@Input(Processor.INPUT) Flux<String> input, @Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> m.toUpperCase()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgs2 {

		@StreamListener(Processor.INPUT)
		public void receive(Flux<String> input, @Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> m.toUpperCase()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithMessage1 {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<Message<?>> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> MessageBuilder
					.withPayload(m.getPayload().toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestWildCardFluxInputOutputArgsWithMessage1 {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<?> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload(m.toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestWildCardFluxInputOutputArgsWithMessage2 {

		@StreamListener(Processor.INPUT)
		public void receive(Flux<?> input, @Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload(m.toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestWildCardFluxInputOutputArgsWithMessage3 {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public void receive(Flux<?> input, FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload(m.toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestWildCardFluxInputOutputArgsWithMessage4 {

		@StreamListener
		@SendTo(Processor.OUTPUT)
		public void receive(@Input(Processor.INPUT) Flux<?> input, FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload(m.toString().toUpperCase()).build()));
		}
	}

	public static class TestGenericStringFluxInputOutputArgsWithMessageImpl1 extends TestGenericFluxInputOutputArgsWithMessage1<String> {
	}

	public static class TestGenericStringFluxInputOutputArgsWithMessageImpl2 extends TestGenericFluxInputOutputArgsWithMessage2<String> {
	}

	public static class TestGenericStringFluxInputOutputArgsWithMessageImpl3 extends TestGenericFluxInputOutputArgsWithMessage3<String> {
	}

	public static class TestGenericStringFluxInputOutputArgsWithMessageImpl4 extends TestGenericFluxInputOutputArgsWithMessage4<String> {
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestGenericFluxInputOutputArgsWithMessage1<A> {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<A> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload((A)m.toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestGenericFluxInputOutputArgsWithMessage2<A> {

		@StreamListener(Processor.INPUT)
		public void receive(Flux<A> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload((A)m.toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestGenericFluxInputOutputArgsWithMessage3<A> {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public void receive(Flux<A> input, FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload((A)m.toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestGenericFluxInputOutputArgsWithMessage4<A> {

		@StreamListener
		@SendTo(Processor.OUTPUT)
		public void receive(@Input(Processor.INPUT) Flux<A> input, FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload((A)m.toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithMessage2 {

		@StreamListener(Processor.INPUT)
		public void receive(Flux<Message<String>> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload(m.getPayload().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithFluxSender1 {
		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<Message<String>> input, @Output(Processor.OUTPUT) FluxSender output) {
			output.send(input
					.map(m -> m.getPayload().toString().toUpperCase())
					.map(o -> MessageBuilder.withPayload(o).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithFluxSender2 {
		@StreamListener(Processor.INPUT)
		public void receive(Flux<Message<?>> input, @Output(Processor.OUTPUT) FluxSender output) {
			output.send(input
					.map(m -> m.getPayload().toString().toUpperCase())
					.map(o -> MessageBuilder.withPayload(o).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithFluxSenderAndFailure1 {
		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<Message<String>> input, @Output(Processor.OUTPUT) FluxSender output) {
			output.send(input
					.map(m -> m.getPayload().toString())
					.map(m -> {
						if (!m.equals("fail")) {
							return m.toUpperCase();
						}
						else {
							throw new RuntimeException();
						}
					})
					.map(o -> MessageBuilder.withPayload(o).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithFluxSenderAndFailure2 {
		@StreamListener(Processor.INPUT)
		public void receive(Flux<Message<?>> input, @Output(Processor.OUTPUT) FluxSender output) {
			output.send(input
					.map(m -> m.getPayload().toString())
					.map(m -> {
						if (!m.equals("fail")) {
							return m.toUpperCase();
						}
						else {
							throw new RuntimeException();
						}
					})
					.map(o -> MessageBuilder.withPayload(o).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturn1 {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
		Flux<String> receive(@Input(Processor.INPUT) Flux<String> input) {
			return input.map(m -> m.toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturn2 {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public Flux<String> receive(Flux<String> input) {
			return input.map(m -> m.toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturn3 {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Flux<String> receive(Flux<String> input) {
			return input.map(m -> m.toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturn4 {

		@StreamListener
		@SendTo(Processor.OUTPUT)
		public Flux<String> receive(@Input(Processor.INPUT) Flux<String> input) {
			return input.map(m -> m.toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithFailure1 {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
		Flux<String> receive(@Input(Processor.INPUT) Flux<String> input) {
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

		@StreamListener(Processor.INPUT)
		public
		@Output(Processor.OUTPUT)
		Flux<String> receive(Flux<String> input) {
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
		Flux<String> receive(Flux<String> input) {
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

		@StreamListener
		public
		@SendTo(Processor.OUTPUT)
		Flux<String> receive(@Input(Processor.INPUT) Flux<String> input) {
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
		Flux<String> receive(@Input(Processor.INPUT) Flux<Message<String>> input) {
			return input.map(m -> m.getPayload().toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithMessage2 {

		@StreamListener(Processor.INPUT)
		public
		@Output(Processor.OUTPUT)
		Flux<String> receive(Flux<Message<String>> input) {
			return input.map(m -> m.getPayload().toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithMessage3 {

		@StreamListener(Processor.INPUT)
		public
		@SendTo(Processor.OUTPUT)
		Flux<String> receive(Flux<Message<String>> input) {
			return input.map(m -> m.getPayload().toUpperCase());
		}
	}


	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithMessage4 {

		@StreamListener
		public
		@SendTo(Processor.OUTPUT)
		Flux<String> receive(@Input(Processor.INPUT) Flux<Message<String>> input) {
			return input.map(m -> m.getPayload().toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithPojo1 {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
		Flux<BarPojo> receive(@Input(Processor.INPUT) Flux<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithPojo2 {

		@StreamListener(Processor.INPUT)
		public
		@Output(Processor.OUTPUT)
		Flux<BarPojo> receive(Flux<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithPojo3 {

		@StreamListener(Processor.INPUT)
		public
		@SendTo(Processor.OUTPUT)
		Flux<BarPojo> receive(Flux<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithPojo4 {

		@StreamListener
		public
		@SendTo(Processor.OUTPUT)
		Flux<BarPojo> receive(@Input(Processor.INPUT) Flux<FooPojo> input) {
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
