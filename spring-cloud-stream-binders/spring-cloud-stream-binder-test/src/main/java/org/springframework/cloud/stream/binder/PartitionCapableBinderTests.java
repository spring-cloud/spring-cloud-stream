/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

/**
 * Tests for binders that support partitioning.
 *
 * @author Gary Russell
 * @author Mark Fisher
 * @author Marius Bogoevici
 */
abstract public class PartitionCapableBinderTests extends BrokerBinderTests {

	@Test
	@SuppressWarnings("unchecked")
	public void testAnonymousGroup() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel output = new DirectChannel();
		Properties properties = new Properties();
		Binding<MessageChannel> producerBinding = binder.bindProducer("defaultGroup.0", output, properties);

		QueueChannel input1 = new QueueChannel();
		Binding<MessageChannel> binding1 = binder.bindConsumer("defaultGroup.0", null, input1, properties);

		QueueChannel input2 = new QueueChannel();
		Binding<MessageChannel> binding2 = binder.bindConsumer("defaultGroup.0", null, input2, properties);

		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));

		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1, not(nullValue()));
		assertThat(new String(receivedMessage1.getPayload()), equalTo(testPayload1));

		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2, not(nullValue()));
		assertThat(new String(receivedMessage2.getPayload()), equalTo(testPayload1));

		binding2.unbind();

		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));

		binding2 = binder.bindConsumer("defaultGroup.0", null, input2, properties);
		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload3.getBytes()));

		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1, not(nullValue()));
		assertThat(new String(receivedMessage1.getPayload()), equalTo(testPayload2));
		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1, not(nullValue()));
		assertThat(new String(receivedMessage1.getPayload()), equalTo(testPayload3));

		receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2, not(nullValue()));
		assertThat(new String(receivedMessage2.getPayload()), equalTo(testPayload3));

		producerBinding.unbind();
		binding1.unbind();
		binding2.unbind();
	}

	@Test
	public void testBadProperties() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();
		properties.put("foo", "bar");
		properties.put("baz", "qux");

		DirectChannel output = new DirectChannel();
		try {
			binder.bindProducer("badprops.0", output, properties);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(Matchers.containsString(getClassUnderTestName()
							+ " does not support producer "),
					containsString("foo"),
					containsString("baz"),
					containsString(" for badprops.0")));
		}

		properties.remove("baz");
		try {
			binder.bindConsumer("badprops.0", "test", output, properties);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), equalTo(getClassUnderTestName()
					+ " does not support consumer property: foo for badprops.0.test."));
		}
	}

	@Test
	public void testPartitionedModuleSpEL() throws Exception {
		Binder<MessageChannel> binder = getBinder();

		Properties consumerProperties = new Properties();
		consumerProperties.put("concurrency", "2");
		consumerProperties.put("partitionIndex", "0");
		consumerProperties.put("count","3");
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0S");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("part.0", "test", input0, consumerProperties);
		consumerProperties.put("partitionIndex", "1");
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1S");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("part.0", "test", input1, consumerProperties);
		consumerProperties.put("partitionIndex", "2");
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2S");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("part.0", "test", input2, consumerProperties);

		Properties producerProperties = new Properties();
		producerProperties.put("partitionKeyExpression", "payload");
		producerProperties.put("partitionSelectorExpression", "hashCode()");
		producerProperties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "3");
		producerProperties.put(BinderPropertyKeys.NEXT_MODULE_CONCURRENCY, "2");

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("part.0", output, producerProperties);
		@SuppressWarnings("unchecked")
		List<Binding<MessageChannel>> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(4, bindings.size());
		try {
			AbstractEndpoint endpoint = ((DefaultBinding<?>)bindings.get(3)).getEndpoint();
			assertThat(getEndpointRouting(endpoint), containsString(
					getExpectedRoutingBaseDestination("part.0", "test") + "-' + headers['partition']"));
		}
		catch (UnsupportedOperationException ignored) {
		}

		Message<Integer> message2 = MessageBuilder.withPayload(2)
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "foo")
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 42)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 43)
				.setHeader(BinderHeaders.BINDER_REPLY_CHANNEL, "bar")
				.build();
		output.send(message2);
		output.send(new GenericMessage<>(1));
		output.send(new GenericMessage<>(0));

		Message<?> receive0 = receive(input0);
		assertNotNull(receive0);
		Message<?> receive1 = receive(input1);
		assertNotNull(receive1);
		Message<?> receive2 = receive(input2);
		assertNotNull(receive2);

		Matcher<Message<?>> fooMatcher = new CustomMatcher<Message<?>>("the message with 'foo' as its correlationId") {

			@Override
			public boolean matches(Object item) {
				IntegrationMessageHeaderAccessor accessor = new IntegrationMessageHeaderAccessor((Message<?>) item);
				boolean result = "foo".equals(accessor.getCorrelationId()) &&
						42 == accessor.getSequenceNumber() &&
						43 == accessor.getSequenceSize() &&
						"bar".equals(accessor.getHeader(BinderHeaders.BINDER_REPLY_CHANNEL));
				return result;
			}
		};
		if (usesExplicitRouting()) {
			assertEquals(0, receive0.getPayload());
			assertEquals(1, receive1.getPayload());
			assertEquals(2, receive2.getPayload());
			assertThat(receive2, fooMatcher);
		}
		else {
			assertThat(Arrays.asList(
					(Integer) receive0.getPayload(),
					(Integer) receive1.getPayload(),
					(Integer) receive2.getPayload()),
					containsInAnyOrder(0, 1, 2));

			@SuppressWarnings("unchecked")
			Matcher<Iterable<? extends Message<?>>> containsOur3Messages = containsInAnyOrder(
					fooMatcher,
					hasProperty("payload", equalTo(0)),
					hasProperty("payload", equalTo(1))
					);
			assertThat(
					Arrays.asList(receive0, receive1, receive2),
					containsOur3Messages);

		}
		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		input2Binding.unbind();
	}

	@Test
	public void testPartitionedModuleJava() throws Exception {
		Binder<MessageChannel> binder = getBinder();

		Properties consumerProperties = new Properties();
		consumerProperties.put("concurrency", "2");
		consumerProperties.put("count","3");
		consumerProperties.put("partitionIndex", "0");
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0J");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partJ.0", "test", input0, consumerProperties);
		consumerProperties.put("partitionIndex", "1");
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1J");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partJ.0", "test", input1, consumerProperties);
		consumerProperties.put("partitionIndex", "2");
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2J");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("partJ.0", "test", input2, consumerProperties);

		Properties producerProperties = new Properties();
		producerProperties.put("partitionKeyExtractorClass", "org.springframework.cloud.stream.binder.PartitionTestSupport");
		producerProperties.put("partitionSelectorClass", "org.springframework.cloud.stream.binder.PartitionTestSupport");
		producerProperties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "3");
		producerProperties.put(BinderPropertyKeys.NEXT_MODULE_CONCURRENCY, "2");
		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partJ.0", output, producerProperties);
		@SuppressWarnings("unchecked")
		List<Binding<MessageChannel>> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(4, bindings.size());
		if (usesExplicitRouting()) {
			AbstractEndpoint endpoint = ((DefaultBinding<?>)bindings.get(3)).getEndpoint();
			assertThat(getEndpointRouting(endpoint), containsString(
					getExpectedRoutingBaseDestination("partJ.0", "test") + "-' + headers['partition']"));
		}

		output.send(new GenericMessage<>(2));
		output.send(new GenericMessage<>(1));
		output.send(new GenericMessage<>(0));

		Message<?> receive0 = receive(input0);
		assertNotNull(receive0);
		Message<?> receive1 = receive(input1);
		assertNotNull(receive1);
		Message<?> receive2 = receive(input2);
		assertNotNull(receive2);

		if (usesExplicitRouting()) {
			assertEquals(0, receive0.getPayload());
			assertEquals(1, receive1.getPayload());
			assertEquals(2, receive2.getPayload());
		}
		else {

			assertThat(Arrays.asList(
					(Integer) receive0.getPayload(),
					(Integer) receive1.getPayload(),
					(Integer) receive2.getPayload()),
					containsInAnyOrder(0, 1, 2));
		}

		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	/**
	 * Implementations should return whether the binder under test uses "explicit" routing (e.g. Rabbit)
	 * whereby XD is responsible for assigning a partition and knows which exact consumer will receive the
	 * message (i.e. honor "partitionIndex") or "implicit" routing (e.g. Kafka) whereby the only guarantee
	 * is that messages will be spread, but we don't control exactly which consumer gets which message.
	 */
	protected abstract boolean usesExplicitRouting();

	/**
	 * For implementations that rely on explicit routing, return the routing expression.
	 */
	protected String getEndpointRouting(AbstractEndpoint endpoint) {
		throw new UnsupportedOperationException();
	}

	/**
	 * For implementations that rely on explicit routing, return the expected base destination
	 * (the part that precedes '-partition' within the expression).
	 */
	protected String getExpectedRoutingBaseDestination(String name, String group) {
		throw new UnsupportedOperationException();
	}

	/**
	 * For implementations that rely on explicit routing, return the routing expression.
	 */
	protected String getPubSubEndpointRouting(AbstractEndpoint endpoint) {
		throw new UnsupportedOperationException();
	}

	protected abstract String getClassUnderTestName();

}
