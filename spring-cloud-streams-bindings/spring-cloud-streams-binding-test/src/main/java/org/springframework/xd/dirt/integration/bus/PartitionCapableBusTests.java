/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.bus;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;


/**
 * Tests for buses that support partitioning.
 *
 * @author Gary Russell
 */
abstract public class PartitionCapableBusTests extends BrokerBusTests {

	@Test
	public void testBadProperties() throws Exception {
		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("foo", "bar");
		properties.put("baz", "qux");

		DirectChannel output = new DirectChannel();
		try {
			bus.bindProducer("badprops.0", output, properties);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), allOf(Matchers.containsString(bus.getClass().getSimpleName().replace("Test", "")
					+ " does not support producer "),
					containsString("foo"),
					containsString("baz"),
					containsString(" for badprops.0.")));
		}

		properties.remove("baz");
		try {
			bus.bindConsumer("badprops.0", output, properties);
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), equalTo(bus.getClass().getSimpleName().replace("Test", "")
					+ " does not support consumer property: foo for badprops.0."));
		}
	}

	@Test
	public void testPartitionedModuleSpEL() throws Exception {
		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("partitionKeyExpression", "payload");
		properties.put("partitionSelectorExpression", "hashCode()");
		properties.put(BusProperties.NEXT_MODULE_COUNT, "3");
		properties.put(BusProperties.NEXT_MODULE_CONCURRENCY, "2");

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		bus.bindProducer("part.0", output, properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);
		assertEquals(1, bindings.size());
		try {
			AbstractEndpoint endpoint = bindings.get(0).getEndpoint();
			assertThat(getEndpointRouting(endpoint), containsString("part.0-' + headers['partition']"));
		}
		catch (UnsupportedOperationException ignored) {

		}

		properties.clear();
		properties.put("concurrency", "2");
		properties.put("partitionIndex", "0");
		properties.put("count","3");
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0S");
		bus.bindConsumer("part.0", input0, properties);
		properties.put("partitionIndex", "1");
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1S");
		bus.bindConsumer("part.0", input1, properties);
		properties.put("partitionIndex", "2");
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2S");
		bus.bindConsumer("part.0", input2, properties);

		Message<Integer> message2 = MessageBuilder.withPayload(2)
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "foo")
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 42)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 43)
				.setHeader("xdReplyChannel", "bar")
				.build();
		output.send(message2);
		output.send(new GenericMessage<Integer>(1));
		output.send(new GenericMessage<Integer>(0));

		Message<?> receive0 = input0.receive(1000);
		assertNotNull(receive0);
		Message<?> receive1 = input1.receive(1000);
		assertNotNull(receive1);
		Message<?> receive2 = input2.receive(1000);
		assertNotNull(receive2);

		Matcher<Message<?>> fooMatcher = new CustomMatcher<Message<?>>("the message with 'foo' as its correlationId") {

			@Override
			public boolean matches(Object item) {
				IntegrationMessageHeaderAccessor accessor = new IntegrationMessageHeaderAccessor((Message<?>) item);
				boolean result = "foo".equals(accessor.getCorrelationId()) &&
						42 == accessor.getSequenceNumber() &&
						43 == accessor.getSequenceSize() &&
						"bar".equals(accessor.getHeader("xdReplyChannel"));
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

		bus.unbindConsumers("part.0");
		bus.unbindProducers("part.0");
	}

	@Test
	public void testPartitionedModuleJava() throws Exception {
		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("partitionKeyExtractorClass", "org.springframework.xd.dirt.integration.bus.PartitionTestSupport");
		properties.put("partitionSelectorClass", "org.springframework.xd.dirt.integration.bus.PartitionTestSupport");
		properties.put(BusProperties.NEXT_MODULE_COUNT, "3");
		properties.put(BusProperties.NEXT_MODULE_CONCURRENCY, "2");

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		bus.bindProducer("partJ.0", output, properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);
		assertEquals(1, bindings.size());
		if (usesExplicitRouting()) {
			AbstractEndpoint endpoint = bindings.get(0).getEndpoint();
			assertThat(getEndpointRouting(endpoint), containsString("partJ.0-' + headers['partition']"));
		}

		properties.clear();
		properties.put("concurrency", "2");
		properties.put("count","3");
		properties.put("partitionIndex", "0");
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0J");
		bus.bindConsumer("partJ.0", input0, properties);
		properties.put("partitionIndex", "1");
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1J");
		bus.bindConsumer("partJ.0", input1, properties);
		properties.put("partitionIndex", "2");
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2J");
		bus.bindConsumer("partJ.0", input2, properties);

		output.send(new GenericMessage<Integer>(2));
		output.send(new GenericMessage<Integer>(1));
		output.send(new GenericMessage<Integer>(0));

		Message<?> receive0 = input0.receive(1000);
		assertNotNull(receive0);
		Message<?> receive1 = input1.receive(1000);
		assertNotNull(receive1);
		Message<?> receive2 = input2.receive(1000);
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

		bus.unbindConsumers("partJ.0");
		bus.unbindProducers("partJ.0");
	}

	/**
	 * Implementations should return whether the bus under test uses "explicit" routing (e.g. Rabbit)
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
	 * For implementations that rely on explicit routing, return the routing expression.
	 */
	protected String getPubSubEndpointRouting(AbstractEndpoint endpoint) {
		throw new UnsupportedOperationException();
	}

}
