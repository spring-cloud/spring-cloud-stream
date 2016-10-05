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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
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
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class StreamListenerReactorTests {

	@Test
	public void testInputOutputArgs() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestInputOutputArgs.class, "--server.port=0");
		sendMessageAndValidate(context);
		context.close();
	}

	private void sendMessageAndValidate(ConfigurableApplicationContext context) throws InterruptedException {
		@SuppressWarnings("unchecked")
		Processor processor = context.getBean(Processor.class);
		String sentPayload = "hello " + UUID.randomUUID().toString();
		processor.input().send(MessageBuilder.withPayload(sentPayload).setHeader("contentType", "text/plain").build());
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Message<?> result = messageCollector.forChannel(processor.output()).poll(1000, TimeUnit.MILLISECONDS);
		assertThat(result).isNotNull();
		assertThat(result.getPayload()).isEqualTo(sentPayload.toUpperCase());
	}

	private void sendFailingMessage(ConfigurableApplicationContext context) throws InterruptedException {
		@SuppressWarnings("unchecked")
		Processor processor = context.getBean(Processor.class);
		processor.input().send(MessageBuilder.withPayload("fail").setHeader("contentType", "text/plain").build());
	}

	@Test
	public void testInputOutputArgsWithMessage() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestInputOutputArgsWithMessage.class,
				"--server.port=0");
		sendMessageAndValidate(context);
		context.close();
	}

	@Test
	public void testWildCardFluxInputOutputArgsWithMessage() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestWildCardFluxInputOutputArgsWithMessage.class,
				"--server.port=0");
		sendMessageAndValidate(context);
		context.close();
	}

	@Test
	public void testGenericFluxInputOutputArgsWithMessage() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestGenericStringFluxInputOutputArgsWithMessage.class,
				"--server.port=0");
		sendMessageAndValidate(context);
		context.close();
	}

	@Test
	public void testInputOutputArgsWithFluxSender() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestInputOutputArgsWithFluxSender.class,
				"--server.port=0");
		// send multiple message
		sendMessageAndValidate(context);
		sendMessageAndValidate(context);
		sendMessageAndValidate(context);
		context.close();
	}

	@Test
	public void testInputOutputArgsWithFluxSenderAndFailure() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestInputOutputArgsWithFluxSenderAndFailure.class, "--server.port=0");
		sendMessageAndValidate(context);
		sendFailingMessage(context);
		sendMessageAndValidate(context);
		context.close();
	}

	@Test
	public void testReturn() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestReturn.class, "--server.port=0");
		sendMessageAndValidate(context);
		sendMessageAndValidate(context);
		sendMessageAndValidate(context);
		context.close();
	}

	@Test
	public void testReturnWithFailure() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestReturnWithFailure.class, "--server.port=0");
		sendMessageAndValidate(context);
		sendFailingMessage(context);
		sendMessageAndValidate(context);
		sendFailingMessage(context);
		sendMessageAndValidate(context);
		context.close();
	}

	@Test
	public void testReturnWithMessage() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestReturnWithMessage.class, "--server.port=0");
		sendMessageAndValidate(context);
		context.close();
	}

	@Test
	public void testReturnWithPojo() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestReturnWithPojo.class, "--server.port=0");
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


	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgs {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<String> input, @Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> m.toUpperCase()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithMessage {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<Message<?>> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> MessageBuilder
					.withPayload(m.getPayload().toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestWildCardFluxInputOutputArgsWithMessage {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<?> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload(m.toString().toUpperCase()).build()));
		}
	}

	public static class TestGenericStringFluxInputOutputArgsWithMessage extends TestGenericFluxInputOutputArgsWithMessage<String> {
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestGenericFluxInputOutputArgsWithMessage<A> {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<A> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> MessageBuilder.withPayload((A)m.toString().toUpperCase()).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithFluxSender {
		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<Message<String>> input, @Output(Processor.OUTPUT) FluxSender output) {
			output.send(input
					.map(m -> m.getPayload().toString().toUpperCase())
					.map(o -> MessageBuilder.withPayload(o).build()));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithFluxSenderAndFailure {
		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<Message<?>> input, @Output(Processor.OUTPUT) FluxSender output) {
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
	public static class TestReturn {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
		Flux<String> receive(@Input(Processor.INPUT) Flux<String> input) {
			return input.map(m -> m.toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithFailure {

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
	public static class TestReturnWithMessage {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
		Flux<String> receive(@Input(Processor.INPUT) Flux<Message<String>> input) {
			return input.map(m -> m.getPayload().toUpperCase());
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnWithPojo {

		@StreamListener
		public
		@Output(Processor.OUTPUT)
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
