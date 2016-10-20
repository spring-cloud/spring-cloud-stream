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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class StreamListenerMethodTests {


	@Test
	public void testMethodWithInputAsMethodAndParameter() throws Exception {
		try {
			SpringApplication.run(TestMethodWithInputAsMethodAndParameter.class, "--server.port=0");
			fail("Exception expected on using Input element as method and parameter");
		}
		catch (BeanCreationException e) {
			assertThat(e.getCause().getMessage()).contains("@Input annotation can not be used in multiple method parameters");
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

	public static class FooPojo {

		private String bar;

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
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
