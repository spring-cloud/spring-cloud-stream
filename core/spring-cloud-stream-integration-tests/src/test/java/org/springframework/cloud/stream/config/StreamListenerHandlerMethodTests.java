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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.annotation.Router;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.Assert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.AMBIGUOUS_MESSAGE_HANDLER_METHOD_ARGUMENTS;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.INPUT_AT_STREAM_LISTENER;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.INVALID_INBOUND_NAME;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.INVALID_OUTBOUND_NAME;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.INVALID_OUTPUT_VALUES;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.NO_INPUT_DESTINATION;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.RETURN_TYPE_MULTIPLE_OUTBOUND_SPECIFIED;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.RETURN_TYPE_NO_OUTBOUND_SPECIFIED;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
public class StreamListenerHandlerMethodTests {

	@Test
	public void testInvalidInputOnMethod() throws Exception {
		try {
			SpringApplication.run(TestInvalidInputOnMethod.class, "--server.port=0",
					"--spring.jmx.enabled=false");
			fail("Exception expected: " + INPUT_AT_STREAM_LISTENER);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(INPUT_AT_STREAM_LISTENER);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMethodWithObjectAsMethodArgument() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestMethodWithObjectAsMethodArgument.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.contentType=text/plain",
				"--spring.cloud.stream.bindings.output.contentType=text/plain");
		Processor processor = context.getBean(Processor.class);
		final String testMessage = "testing";
		processor.input().send(MessageBuilder.withPayload(testMessage).build());
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Message<String> result = (Message<String>) messageCollector
				.forChannel(processor.output()).poll(1000, TimeUnit.MILLISECONDS);
		assertThat(result).isNotNull();
		assertThat(result.getPayload()).isEqualTo(testMessage.toUpperCase());
		context.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	/**
	 * @since 2.0 : This test is an example of the new behavior of 2.0 when it comes to
	 * contentType handling. The default contentType being JSON in order to be able to
	 * check a message without quotes the user needs to set the input/output contentType
	 * accordingly Also, received messages are always of Message<byte[]> now.
	 */
	public void testMethodHeadersPropagatged() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestMethodHeadersPropagated.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.contentType=text/plain",
				"--spring.cloud.stream.bindings.output.contentType=text/plain");
		Processor processor = context.getBean(Processor.class);
		final String testMessage = "testing";
		processor.input().send(
				MessageBuilder.withPayload(testMessage).setHeader("foo", "bar").build());
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Message<String> result = (Message<String>) messageCollector
				.forChannel(processor.output()).poll(1000, TimeUnit.MILLISECONDS);
		assertThat(result).isNotNull();
		assertThat(result.getPayload()).isEqualTo(testMessage.toUpperCase());
		assertThat(result.getHeaders().get("foo")).isEqualTo("bar");
		context.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	@Disabled
	public void testMethodHeadersNotPropagatged() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestMethodHeadersNotPropagated.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.contentType=text/plain",
				"--spring.cloud.stream.bindings.output.contentType=text/plain");
		Processor processor = context.getBean(Processor.class);
		final String testMessage = "testing";
		processor.input().send(
				MessageBuilder.withPayload(testMessage).setHeader("foo", "bar").build());
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Message<String> result = (Message<String>) messageCollector
				.forChannel(processor.output()).poll(1000, TimeUnit.MILLISECONDS);
		assertThat(result).isNotNull();
		assertThat(result.getPayload()).isEqualTo(testMessage.toUpperCase());
		assertThat(result.getHeaders().get("foo")).isNull();
		context.close();
	}

	// TODO: Handle dynamic destinations and contentType
	@SuppressWarnings("unchecked")
	public void testStreamListenerMethodWithTargetBeanFromOutside() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestStreamListenerMethodWithTargetBeanFromOutside.class,
				"--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.contentType=text/plain",
				"--spring.cloud.stream.bindings.output.contentType=text/plain");
		Sink sink = context.getBean(Sink.class);
		final String testMessageToSend = "testing";
		sink.input().send(MessageBuilder.withPayload(testMessageToSend).build());
		DirectChannel directChannel = (DirectChannel) context
				.getBean(testMessageToSend.toUpperCase(), MessageChannel.class);
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Message<String> result = (Message<String>) messageCollector
				.forChannel(directChannel).poll(1000, TimeUnit.MILLISECONDS);
		sink.input().send(MessageBuilder.withPayload(testMessageToSend).build());
		assertThat(result).isNotNull();
		assertThat(result.getPayload()).isEqualTo(testMessageToSend.toUpperCase());
		context.close();
	}

	@Test
	public void testInvalidReturnTypeWithSendToAndOutput() throws Exception {
		try {
			SpringApplication.run(TestReturnTypeWithMultipleOutput.class,
					"--server.port=0", "--spring.jmx.enabled=false");
			fail("Exception expected: " + RETURN_TYPE_MULTIPLE_OUTBOUND_SPECIFIED);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(RETURN_TYPE_MULTIPLE_OUTBOUND_SPECIFIED);
		}
	}

	@Test
	public void testInvalidReturnTypeWithNoOutput() throws Exception {
		try {
			SpringApplication.run(TestInvalidReturnTypeWithNoOutput.class,
					"--server.port=0", "--spring.jmx.enabled=false");
			fail("Exception expected: " + RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
		}
	}

	@Test
	public void testInvalidInputAnnotationWithNoValue() throws Exception {
		try {
			SpringApplication.run(TestInvalidInputAnnotationWithNoValue.class,
					"--server.port=0", "--spring.jmx.enabled=false");
			fail("Exception expected: " + INVALID_INBOUND_NAME);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(INVALID_INBOUND_NAME);
		}
	}

	@Test
	public void testInvalidOutputAnnotationWithNoValue() throws Exception {
		try {
			SpringApplication.run(TestInvalidOutputAnnotationWithNoValue.class,
					"--server.port=0", "--spring.jmx.enabled=false");
			fail("Exception expected: " + INVALID_OUTBOUND_NAME);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(INVALID_OUTBOUND_NAME);
		}
	}

	@Test
	public void testMethodInvalidInboundName() throws Exception {
		try {
			SpringApplication.run(TestMethodInvalidInboundName.class, "--server.port=0",
					"--spring.jmx.enabled=false");
			fail("Exception expected on using invalid inbound name");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(
					StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
	}

	@Test
	public void testMethodInvalidOutboundName() throws Exception {
		try {
			SpringApplication.run(TestMethodInvalidOutboundName.class, "--server.port=0",
					"--spring.jmx.enabled=false");
			fail("Exception expected on using invalid outbound name");
		}
		catch (NoSuchBeanDefinitionException e) {
			assertThat(e.getMessage()).contains("invalid");
		}
	}

	@Test
	public void testAmbiguousMethodArguments1() throws Exception {
		try {
			SpringApplication.run(TestAmbiguousMethodArguments1.class, "--server.port=0",
					"--spring.jmx.enabled=false");
			fail("Exception expected: " + AMBIGUOUS_MESSAGE_HANDLER_METHOD_ARGUMENTS);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage())
					.contains(AMBIGUOUS_MESSAGE_HANDLER_METHOD_ARGUMENTS);
		}
	}

	@Test
	public void testAmbiguousMethodArguments2() throws Exception {
		try {
			SpringApplication.run(TestAmbiguousMethodArguments2.class, "--server.port=0",
					"--spring.jmx.enabled=false");
			fail("Exception expected:" + AMBIGUOUS_MESSAGE_HANDLER_METHOD_ARGUMENTS);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage())
					.contains(AMBIGUOUS_MESSAGE_HANDLER_METHOD_ARGUMENTS);
		}
	}

	@Test
	public void testMethodWithInputAsMethodAndParameter() throws Exception {
		try {
			SpringApplication.run(TestMethodWithInputAsMethodAndParameter.class,
					"--server.port=0", "--spring.jmx.enabled=false");
			fail("Exception expected: " + INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
	}

	@Test
	public void testMethodWithOutputAsMethodAndParameter() throws Exception {
		try {
			SpringApplication.run(TestMethodWithOutputAsMethodAndParameter.class,
					"--server.port=0", "--spring.jmx.enabled=false");
			fail("Exception expected:" + INVALID_OUTPUT_VALUES);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).startsWith(INVALID_OUTPUT_VALUES);
		}
	}

	@Test
	public void testMethodWithoutInput() throws Exception {
		try {
			SpringApplication.run(TestMethodWithoutInput.class, "--server.port=0",
					"--spring.jmx.enabled=false");
			fail("Exception expected when inbound target is not set");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains(NO_INPUT_DESTINATION);
		}
	}

	@Test
	public void testMethodWithMultipleInputParameters() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestMethodWithMultipleInputParameters.class, "--server.port=0",
				"--spring.jmx.enabled=false");
		Processor processor = context.getBean(Processor.class);
		StreamListenerTestUtils.FooInboundChannel1 inboundChannel2 = context
				.getBean(StreamListenerTestUtils.FooInboundChannel1.class);
		final CountDownLatch latch = new CountDownLatch(2);
		((SubscribableChannel) processor.output()).subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				Assert.isTrue(
						message.getPayload().equals("footesting")
								|| message.getPayload().equals("BARTESTING"),
						"Assert failed");
				latch.countDown();
			}
		});
		processor.input().send(MessageBuilder.withPayload("{\"foo\":\"fooTESTing\"}")
				.setHeader("contentType", "application/json").build());
		inboundChannel2.input()
				.send(MessageBuilder.withPayload("{\"bar\":\"bartestING\"}")
						.setHeader("contentType", "application/json").build());
		assertThat(latch.await(1, TimeUnit.SECONDS));
		context.close();
	}

	@Test
	public void testMethodWithMultipleOutputParameters() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(
				TestMethodWithMultipleOutputParameters.class, "--server.port=0",
				"--spring.jmx.enabled=false");
		Processor processor = context.getBean(Processor.class);
		StreamListenerTestUtils.FooOutboundChannel1 source2 = context
				.getBean(StreamListenerTestUtils.FooOutboundChannel1.class);
		final CountDownLatch latch = new CountDownLatch(2);
		((SubscribableChannel) processor.output()).subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				Assert.isTrue(message.getPayload().equals("testing"), "Assert failed");
				Assert.isTrue(message.getHeaders().get("output").equals("output2"),
						"Assert failed");
				latch.countDown();
			}
		});
		((SubscribableChannel) source2.output()).subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				Assert.isTrue(message.getPayload().equals("TESTING"), "Assert failed");
				Assert.isTrue(message.getHeaders().get("output").equals("output1"),
						"Assert failed");
				latch.countDown();
			}
		});
		processor.input().send(MessageBuilder.withPayload("testING")
				.setHeader("output", "output1").build());
		processor.input().send(MessageBuilder.withPayload("TESTing")
				.setHeader("output", "output2").build());
		assertThat(latch.await(1, TimeUnit.SECONDS));
		context.close();
	}

	@EnableBinding({ Processor.class, StreamListenerTestUtils.FooOutboundChannel1.class })
	@EnableAutoConfiguration
	public static class TestMethodWithMultipleOutputParameters {

		@StreamListener
		public void receive(@Input(Processor.INPUT) SubscribableChannel input,
				@Output(Processor.OUTPUT) final MessageChannel output1,
				@Output(StreamListenerTestUtils.FooOutboundChannel1.OUTPUT) final MessageChannel output2) {
			input.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					if (message.getHeaders().get("output").equals("output1")) {
						output1.send(org.springframework.messaging.support.MessageBuilder
								.withPayload(
										message.getPayload().toString().toUpperCase())
								.build());
					}
					else if (message.getHeaders().get("output").equals("output2")) {
						output2.send(org.springframework.messaging.support.MessageBuilder
								.withPayload(
										message.getPayload().toString().toLowerCase())
								.build());
					}
				}
			});
		}

	}

	@EnableBinding({ Sink.class })
	@EnableAutoConfiguration
	public static class TestMethodWithoutInput {

		@StreamListener
		public void receive(StreamListenerTestUtils.FooPojo fooPojo) {
		}

	}

	@EnableBinding({ Processor.class })
	@EnableAutoConfiguration
	public static class TestMethodWithObjectAsMethodArgument {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String receive(Object received) {
			return received.toString().toUpperCase();
		}

	}

	@EnableBinding({ Processor.class })
	@EnableAutoConfiguration
	public static class TestMethodHeadersPropagated {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String receive(String received) {
			return received.toUpperCase();
		}

	}

	@EnableBinding({ Processor.class })
	@EnableAutoConfiguration
	public static class TestMethodHeadersNotPropagated {

		@StreamListener(value = Processor.INPUT, copyHeaders = "${foo.bar:false}")
		@SendTo(Processor.OUTPUT)
		public String receive(String received) {
			return received.toUpperCase();
		}

	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestStreamListenerMethodWithTargetBeanFromOutside {

		private static final String ROUTER_QUEUE = "routeInstruction";

		@StreamListener(Sink.INPUT)
		@SendTo(ROUTER_QUEUE)
		public Message<String> convertMessageBody(Message<String> message) {
			return new DefaultMessageBuilderFactory()
					.withPayload(message.getPayload().toUpperCase()).build();
		}

		@Router(inputChannel = ROUTER_QUEUE)
		public String route(String message) {
			return message.toUpperCase();
		}

	}

	@EnableBinding({ Sink.class })
	@EnableAutoConfiguration
	public static class TestInvalidInputOnMethod {

		@StreamListener
		@Input(Sink.INPUT)
		public void receive(StreamListenerTestUtils.FooPojo fooPojo) {
		}

	}

	@EnableBinding({ Sink.class })
	@EnableAutoConfiguration
	public static class TestAmbiguousMethodArguments1 {

		@StreamListener(Processor.INPUT)
		public void receive(@Payload StreamListenerTestUtils.FooPojo fooPojo,
				String value) {
		}

	}

	@EnableBinding({ Sink.class })
	@EnableAutoConfiguration
	public static class TestAmbiguousMethodArguments2 {

		@StreamListener(Processor.INPUT)
		public void receive(@Payload StreamListenerTestUtils.FooPojo fooPojo,
				@Payload StreamListenerTestUtils.BarPojo barPojo) {
		}

	}

	@EnableBinding({ Processor.class, StreamListenerTestUtils.FooOutboundChannel1.class })
	@EnableAutoConfiguration
	public static class TestReturnTypeWithMultipleOutput {

		@StreamListener
		public String receive(@Input(Processor.INPUT) SubscribableChannel input1,
				@Output(Processor.OUTPUT) MessageChannel output1,
				@Output(StreamListenerTestUtils.FooOutboundChannel1.OUTPUT) MessageChannel output2) {
			return "foo";
		}

	}

	@EnableBinding({ Processor.class, StreamListenerTestUtils.FooOutboundChannel1.class })
	@EnableAutoConfiguration
	public static class TestInvalidReturnTypeWithNoOutput {

		@StreamListener
		public String receive(@Input(Processor.INPUT) SubscribableChannel input1) {
			return "foo";
		}

	}

	@EnableBinding({ Processor.class })
	@EnableAutoConfiguration
	public static class TestInvalidInputAnnotationWithNoValue {

		@StreamListener
		public void receive(@Input SubscribableChannel input) {
		}

	}

	@EnableBinding({ Processor.class })
	@EnableAutoConfiguration
	public static class TestInvalidOutputAnnotationWithNoValue {

		@StreamListener
		public void receive(@Input(Processor.OUTPUT) SubscribableChannel input,
				@Output MessageChannel output) {
		}

	}

	@EnableBinding({ Sink.class })
	@EnableAutoConfiguration
	public static class TestMethodInvalidInboundName {

		@StreamListener
		public void receive(@Input("invalid") SubscribableChannel input) {
		}

	}

	@EnableBinding({ Processor.class })
	@EnableAutoConfiguration
	public static class TestMethodInvalidOutboundName {

		@StreamListener
		public void receive(@Input(Processor.INPUT) SubscribableChannel input,
				@Output("invalid") MessageChannel output) {
		}

	}

	@EnableBinding({ Sink.class })
	@EnableAutoConfiguration
	public static class TestMethodWithInputAsMethodAndParameter {

		@StreamListener
		public void receive(@Input(Sink.INPUT) StreamListenerTestUtils.FooPojo fooPojo) {
		}

	}

	@EnableBinding({ Processor.class, StreamListenerTestUtils.FooOutboundChannel1.class })
	@EnableAutoConfiguration
	public static class TestMethodWithOutputAsMethodAndParameter {

		@StreamListener
		@Output(StreamListenerTestUtils.FooOutboundChannel1.OUTPUT)
		public void receive(@Input(Processor.INPUT) SubscribableChannel input,
				@Output(Processor.OUTPUT) final MessageChannel output1) {
			input.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					output1.send(org.springframework.messaging.support.MessageBuilder
							.withPayload(message.getPayload().toString().toUpperCase())
							.build());
				}
			});
		}

	}

	@EnableBinding({ Processor.class, StreamListenerTestUtils.FooInboundChannel1.class })
	@EnableAutoConfiguration
	public static class TestMethodWithMultipleInputParameters {

		@StreamListener
		public void receive(@Input(Processor.INPUT) SubscribableChannel input1,
				@Input(StreamListenerTestUtils.FooInboundChannel1.INPUT) SubscribableChannel input2,
				final @Output(Processor.OUTPUT) MessageChannel output) {
			input1.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					output.send(org.springframework.messaging.support.MessageBuilder
							.withPayload(message.getPayload().toString().toUpperCase())
							.build());
				}
			});
			input2.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					output.send(org.springframework.messaging.support.MessageBuilder
							.withPayload(message.getPayload().toString().toUpperCase())
							.build());
				}
			});
		}

	}

}
