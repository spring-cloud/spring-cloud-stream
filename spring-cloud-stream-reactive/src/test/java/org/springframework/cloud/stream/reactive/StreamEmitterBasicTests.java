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

package org.springframework.cloud.stream.reactive;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.core.publisher.Flux;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
public class StreamEmitterBasicTests {

	@Test
	public void testFluxReturnAndOutputMethodLevel() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestFluxReturnAndOutputMethodLevel.class, "--server.port=0");
		receiveAndValidate(context);
		context.close();
	}

	@Test
	public void testVoidReturnAndOutputMethodParameter() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestVoidReturnAndOutputMethodParameter.class, "--server.port=0");
		receiveAndValidate(context);
		context.close();
	}

	@Test
	public void testVoidReturnAndOutputAtMethodLevel() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestVoidReturnAndOutputAtMethodLevel.class, "--server.port=0");
		receiveAndValidate(context);
		context.close();
	}

	@Test
	public void testVoidReturnAndMultipleOutputMethodParameters() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestVoidReturnAndMultipleOutputMethodParameters.class, "--server.port=0");
		receiveAndValidateMultipleOutputs(context);
		context.close();
	}

	@Test
	public void testFluxReturnAndOutputMethodLevelX() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(TestFluxReturnAndOutputMethodLevelX.class, "--server.port=0");
		receiveAndValidateMultipleOutputs(context);
		context.close();
	}

	@SuppressWarnings("unchecked")
	private static void receiveAndValidate(ConfigurableApplicationContext context) throws InterruptedException {
		Source source = context.getBean(Source.class);
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		List<String> messages = new ArrayList<>();
		for (int i = 0; i < 1000; i ++) {
			messages.add((String)messageCollector.forChannel(source.output()).poll(5000, TimeUnit.MILLISECONDS).getPayload());
		}
		for (int i = 0; i < 1000; i ++) {
			assertThat(messages.get(i)).isEqualTo("Hello World!!" + i);
		}
	}

	@SuppressWarnings("unchecked")
	private static void receiveAndValidateMultipleOutputs(ConfigurableApplicationContext context) throws InterruptedException {
		TestMultiOutboundChannels source = context.getBean(TestMultiOutboundChannels.class);
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		List<String> messages = new ArrayList<>();
		assertMessages(source.output1(), messageCollector, messages);
		messages.clear();
		assertMessages(source.output2(), messageCollector, messages);
		messages.clear();
		assertMessages(source.output3(), messageCollector, messages);
		messages.clear();
	}

	private static void assertMessages(MessageChannel channel, MessageCollector messageCollector, List<String> messages) throws InterruptedException {
		for (int i = 0; i < 1000; i ++) {
			messages.add((String)messageCollector.forChannel(channel).poll(5000, TimeUnit.MILLISECONDS).getPayload());
		}
		for (int i = 0; i < 1000; i ++) {
			assertThat(messages.get(i)).isEqualTo("Hello World!!" + i);
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestFluxReturnAndOutputMethodLevel {

		@StreamEmitter
		@Output(Source.OUTPUT)
		public Flux<String> emit() {
			return Flux.intervalMillis(1)
					.map(l -> "Hello World!!" + l);
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestVoidReturnAndOutputMethodParameter {

		@StreamEmitter
		public void emit(@Output(Source.OUTPUT) FluxSender output) {
			output.send(Flux.intervalMillis(1)
					.map(l -> "Hello World!!" + l));
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestVoidReturnAndOutputAtMethodLevel {

		@StreamEmitter
		@Output(Source.OUTPUT)
		public void emit(FluxSender output) {
			output.send(Flux.intervalMillis(1)
					.map(l -> "Hello World!!" + l));
		}
	}

	@EnableBinding(TestMultiOutboundChannels.class)
	@EnableAutoConfiguration
	public static class TestVoidReturnAndMultipleOutputMethodParameters {

		@StreamEmitter
		public void emit(@Output(TestMultiOutboundChannels.OUTPUT1) FluxSender output1,
						@Output(TestMultiOutboundChannels.OUTPUT2) FluxSender output2,
						@Output(TestMultiOutboundChannels.OUTPUT3) FluxSender output3) {
			output1.send(Flux.intervalMillis(1)
					.map(l -> "Hello World!!" + l));
			output2.send(Flux.intervalMillis(1)
					.map(l -> "Hello World!!" + l));
			output3.send(Flux.intervalMillis(1)
					.map(l -> "Hello World!!" + l));
		}
	}

	@EnableBinding(TestMultiOutboundChannels.class)
	@EnableAutoConfiguration
	public static class TestFluxReturnAndOutputMethodLevelX {

		@StreamEmitter
		@Output(TestMultiOutboundChannels.OUTPUT1)
		public Flux<String> emit1() {
			return Flux.intervalMillis(1)
					.map(l -> "Hello World!!" + l);
		}

		@StreamEmitter
		@Output(TestMultiOutboundChannels.OUTPUT2)
		public Flux<String> emit2() {
			return Flux.intervalMillis(1)
					.map(l -> "Hello World!!" + l);
		}

		@StreamEmitter
		public void emit3(@Output(TestMultiOutboundChannels.OUTPUT3) FluxSender outputX) {
			outputX.send(Flux.intervalMillis(1)
					.map(l -> "Hello World!!" + l));
		}
	}

	interface TestMultiOutboundChannels {

		String OUTPUT1 = "output1";
		String OUTPUT2 = "output2";
		String OUTPUT3 = "output3";

		@Output(TestMultiOutboundChannels.OUTPUT1)
		MessageChannel output1();

		@Output(TestMultiOutboundChannels.OUTPUT2)
		MessageChannel output2();

		@Output(TestMultiOutboundChannels.OUTPUT3)
		MessageChannel output3();

	}
}
