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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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

/**
 * @author Ilayaperumal Gopinathan
 */
@RunWith(Parameterized.class)
public class StreamListenerTestGenericFluxInputOutputArgsWithMessage {

	private Class<?> configClass;

	public StreamListenerTestGenericFluxInputOutputArgsWithMessage(Class<?> configClass) {
		this.configClass = configClass;
	}

	@Parameterized.Parameters
	public static Collection InputConfigs() {
		return Arrays.asList(new Class[] {TestGenericStringFluxInputOutputArgsWithMessageImpl1.class});
	}

	@Test
	public void testGenericFluxInputOutputArgsWithMessage() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(this.configClass, "--server.port=0");
		sendMessageAndValidate(context);
		context.close();
	}

	@Test
	public void testIncorrectUsage1() {
		try {
			SpringApplication.run(TestGenericStringFluxInputOutputArgsWithMessageImpl2.class, "--server.port=0");
			fail("IllegalArgumentException should have been thrown");
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains("@Input annotation should be specified as method parameter instead of " +
					"StreamListener value when specifying @Output/@SendTo annotation");
		}
	}

	@Test
	public void testIncorrectUsage2() {
		try {
			SpringApplication.run(TestGenericStringFluxInputOutputArgsWithMessageImpl3.class, "--server.port=0");
			fail("IllegalArgumentException should have been thrown");
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains("The StreamListener method with declarative inbound and outbound values " +
					"should have @Input and @Output annotations specified as method parameters");
		}
	}

	@Test
	public void testIncorrectUsage3() {
		try {
			SpringApplication.run(TestGenericStringFluxInputOutputArgsWithMessageImpl4.class, "--server.port=0");
			fail("IllegalArgumentException should have been thrown");
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains("Declarative StreamListener method with both " +
					"inbound and outbound values should have @Input and @Output annotations specified as method parameters");
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
}
