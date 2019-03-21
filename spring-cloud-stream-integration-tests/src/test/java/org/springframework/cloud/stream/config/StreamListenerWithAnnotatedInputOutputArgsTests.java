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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
public class StreamListenerWithAnnotatedInputOutputArgsTests {

	@Test
	public void testInputOutputArgs() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestInputOutputArgs.class, "--server.port=0",
				"--spring.cloud.stream.bindings.output.contentType=text/plain",
				"--spring.jmx.enabled=false");
		sendMessageAndValidate(context);
	}

	@Test
	public void testInputOutputArgsWithMoreParameters() {
		try {
			SpringApplication.run(TestInputOutputArgsWithMoreParameters.class,
					"--server.port=0");
			fail("Expected exception: " + INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
	}

	@Test
	public void testInputOutputArgsWithInvalidBindableTarget() {
		try {
			SpringApplication.run(TestInputOutputArgsWithInvalidBindableTarget.class,
					"--server.port=0", "--spring.jmx.enabled=false");
			fail("Exception expected on using invalid bindable target as method parameter");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(
					StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
	}

	@Test
	public void testInputOutputArgsWithParameterOrderChanged() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestInputOutputArgsWithParameterOrderChanged.class, "--server.port=0",
				"--spring.cloud.stream.bindings.output.contentType=text/plain",
				"--spring.jmx.enabled=false");
		sendMessageAndValidate(context);
	}

	@SuppressWarnings("unchecked")
	private void sendMessageAndValidate(ConfigurableApplicationContext context)
			throws InterruptedException {
		Processor processor = context.getBean(Processor.class);
		processor.input().send(MessageBuilder.withPayload("hello")
				.setHeader("contentType", "text/plain").build());
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Message<String> result = (Message<String>) messageCollector
				.forChannel(processor.output()).poll(1000, TimeUnit.MILLISECONDS);
		assertThat(result).isNotNull();
		assertThat(result.getPayload()).isEqualTo("HELLO");
		context.close();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgs {

		@StreamListener
		public void receive(@Input(Processor.INPUT) SubscribableChannel input,
				@Output(Processor.OUTPUT) final MessageChannel output) {
			input.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					output.send(MessageBuilder
							.withPayload(message.getPayload().toString().toUpperCase())
							.build());
				}
			});
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithMoreParameters {

		@StreamListener
		public void receive(@Input(Processor.INPUT) SubscribableChannel input,
				@Output(Processor.OUTPUT) final MessageChannel output, String someArg) {
			input.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					output.send(MessageBuilder
							.withPayload(message.getPayload().toString().toUpperCase())
							.build());
				}
			});
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithInvalidBindableTarget {

		@StreamListener
		public void receive(@Input("invalid") SubscribableChannel input,
				@Output(Processor.OUTPUT) final MessageChannel output) {
			input.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					output.send(MessageBuilder
							.withPayload(message.getPayload().toString().toUpperCase())
							.build());
				}
			});
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputOutputArgsWithParameterOrderChanged {

		@StreamListener
		public void receive(@Output(Processor.OUTPUT) final MessageChannel output,
				@Input("input") SubscribableChannel input) {
			input.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					output.send(MessageBuilder
							.withPayload(message.getPayload().toString().toUpperCase())
							.build());
				}
			});
		}

	}

}
