/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.xd.dirt.integration.bus.kafka;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.channel.interceptor.WireTap;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.xd.dirt.integration.bus.Binding;
import org.springframework.xd.dirt.integration.bus.BusProperties;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.integration.bus.XdHeaders;
import org.springframework.xd.dirt.integration.kafka.KafkaMessageBus;

/**
 * @author Marius Bogoevici
 */
@Ignore //TODO: Fix this test
public class RawModeKafkaMessageBusTests extends KafkaMessageBusTests {

	@Override
	protected KafkaTestMessageBus createKafkaTestMessageBus() {
		return new KafkaTestMessageBus(kafkaTestSupport, getCodec(), KafkaMessageBus.Mode.raw);
	}

	@Test
	@Override
	public void testPartitionedModuleJava() throws Exception {
		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("partitionKeyExtractorClass", "org.springframework.xd.dirt.integration.bus.kafka.RawKafkaPartitionTestSupport");
		properties.put("partitionSelectorClass", "org.springframework.xd.dirt.integration.bus.kafka.RawKafkaPartitionTestSupport");
		properties.put(BusProperties.NEXT_MODULE_COUNT, "3");
		properties.put(BusProperties.NEXT_MODULE_CONCURRENCY, "2");

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		bus.bindProducer("partJ.0", output, properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(bus, "messageBus.bindings", List.class);
		assertEquals(1, bindings.size());

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

		output.send(new GenericMessage<>(new byte[]{(byte)0}));
		output.send(new GenericMessage<>(new byte[]{(byte)1}));
		output.send(new GenericMessage<>(new byte[]{(byte)2}));

		Message<?> receive0 = input0.receive(1000);
		assertNotNull(receive0);
		Message<?> receive1 = input1.receive(1000);
		assertNotNull(receive1);
		Message<?> receive2 = input2.receive(1000);
		assertNotNull(receive2);

		assertThat(Arrays.asList(
						((byte[]) receive0.getPayload())[0],
						((byte[]) receive1.getPayload())[0],
						((byte[]) receive2.getPayload())[0]),
				containsInAnyOrder((byte)0, (byte)1, (byte)2));

		bus.unbindConsumers("partJ.0");
		bus.unbindProducers("partJ.0");
	}

	@Test
	@Override
	public void testPartitionedModuleSpEL() throws Exception {
		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.put("partitionKeyExpression", "payload[0]");
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

		Message<byte[]> message2 = MessageBuilder.withPayload(new byte[]{2})
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "foo")
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 42)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 43)
				.setHeader("xdReplyChannel", "bar")
				.build();
		output.send(message2);
		output.send(new GenericMessage<>(new byte[]{1}));
		output.send(new GenericMessage<>(new byte[]{0}));

		Message<?> receive0 = input0.receive(1000);
		assertNotNull(receive0);
		Message<?> receive1 = input1.receive(1000);
		assertNotNull(receive1);
		Message<?> receive2 = input2.receive(1000);
		assertNotNull(receive2);


		assertThat(Arrays.asList(
						((byte[]) receive0.getPayload())[0],
						((byte[]) receive1.getPayload())[0],
						((byte[]) receive2.getPayload())[0]),
				containsInAnyOrder((byte)0, (byte)1, (byte)2));

		bus.unbindConsumers("part.0");
		bus.unbindProducers("part.0");
	}

	@Test
	@Override
	public void createInboundPubSubBeforeOutboundPubSub() throws Exception {
		MessageBus messageBus = getMessageBus();
		DirectChannel moduleOutputChannel = new DirectChannel();
		// Test pub/sub by emulating how StreamPlugin handles taps
		DirectChannel tapChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		// Create the tap first
		String fooTapName = messageBus.isCapable(MessageBus.Capability.DURABLE_PUBSUB) ? "foo.tap:baz.http" : "tap:baz.http";
		messageBus.bindPubSubConsumer(fooTapName, module2InputChannel, null);

		// Then create the stream
		messageBus.bindProducer("baz.0", moduleOutputChannel, null);
		messageBus.bindConsumer("baz.0", moduleInputChannel, null);
		moduleOutputChannel.addInterceptor(new WireTap(tapChannel));
		messageBus.bindPubSubProducer("tap:baz.http", tapChannel, null);

		// Another new module is using tap as an input channel
		String barTapName = messageBus.isCapable(MessageBus.Capability.DURABLE_PUBSUB) ? "bar.tap:baz.http" : "tap:baz.http";
		messageBus.bindPubSubConsumer(barTapName, module3InputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo".getBytes()).setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar").build();
		boolean success = false;
		boolean retried = false;
		while (!success) {
			moduleOutputChannel.send(message);
			Message<?> inbound = moduleInputChannel.receive(5000);
			assertNotNull(inbound);
			assertEquals("foo", new String((byte[])inbound.getPayload()));
			Message<?> tapped1 = module2InputChannel.receive(5000);
			Message<?> tapped2 = module3InputChannel.receive(5000);
			if (tapped1 == null || tapped2 == null) {
				// listener may not have started
				assertFalse("Failed to receive tap after retry", retried);
				retried = true;
				continue;
			}
			success = true;
			assertEquals("foo", new String((byte[]) tapped1.getPayload()));
			assertNull(tapped1.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo", new String((byte[])tapped2.getPayload()));
		}
		// delete one tap stream is deleted
		messageBus.unbindConsumer(barTapName, module3InputChannel);
		Message<?> message2 = MessageBuilder.withPayload("bar".getBytes()).setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar").build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = module2InputChannel.receive(5000);
		assertNotNull(tapped);

		// Removed tap does not
		assertNull(module3InputChannel.receive(1000));

		// when other tap stream is deleted
		messageBus.unbindConsumer(fooTapName, module2InputChannel);
		// Clean up as StreamPlugin would
		messageBus.unbindConsumer("baz.0", moduleInputChannel);
		messageBus.unbindProducer("baz.0", moduleOutputChannel);
		messageBus.unbindProducers("tap:baz.http");
		assertTrue(getBindings(messageBus).isEmpty());
	}

	@Test
	@Override
	public void testSendAndReceive() throws Exception {
		MessageBus messageBus = getMessageBus();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		messageBus.bindProducer("foo.0", moduleOutputChannel, null);
		messageBus.bindConsumer("foo.0", moduleInputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo".getBytes()).build();
		// Let the consumer actually bind to the producer before sending a msg
		busBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(5000);
		assertNotNull(inbound);
		assertEquals("foo", new String((byte[])inbound.getPayload()));
		messageBus.unbindProducers("foo.0");
		messageBus.unbindConsumers("foo.0");
	}

	// Ignored, since raw mode does not support headers
	@Test
	@Override
	@Ignore
	public void testSendAndReceiveNoOriginalContentType() throws Exception {

	}

	@Test
	public void testSendAndReceivePubSub() throws Exception {
		MessageBus messageBus = getMessageBus();
		DirectChannel moduleOutputChannel = new DirectChannel();
		// Test pub/sub by emulating how StreamPlugin handles taps
		DirectChannel tapChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		messageBus.bindProducer("baz.0", moduleOutputChannel, null);
		messageBus.bindConsumer("baz.0", moduleInputChannel, null);
		moduleOutputChannel.addInterceptor(new WireTap(tapChannel));
		messageBus.bindPubSubProducer("tap:baz.http", tapChannel, null);
		// A new module is using the tap as an input channel
		String fooTapName = messageBus.isCapable(MessageBus.Capability.DURABLE_PUBSUB) ? "foo.tap:baz.http" : "tap:baz.http";
		messageBus.bindPubSubConsumer(fooTapName, module2InputChannel, null);
		// Another new module is using tap as an input channel
		String barTapName = messageBus.isCapable(MessageBus.Capability.DURABLE_PUBSUB) ? "bar.tap:baz.http" : "tap:baz.http";
		messageBus.bindPubSubConsumer(barTapName, module3InputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo".getBytes()).build();
		boolean success = false;
		boolean retried = false;
		while (!success) {
			moduleOutputChannel.send(message);
			Message<?> inbound = moduleInputChannel.receive(5000);
			assertNotNull(inbound);
			assertEquals("foo", new String((byte[])inbound.getPayload()));

			Message<?> tapped1 = module2InputChannel.receive(5000);
			Message<?> tapped2 = module3InputChannel.receive(5000);
			if (tapped1 == null || tapped2 == null) {
				// listener may not have started
				assertFalse("Failed to receive tap after retry", retried);
				retried = true;
				continue;
			}
			success = true;
			assertEquals("foo", new String((byte[])tapped1.getPayload()));
			assertEquals("foo", new String((byte[])tapped2.getPayload()));
		}
		// delete one tap stream is deleted
		messageBus.unbindConsumer(barTapName, module3InputChannel);
		Message<?> message2 = MessageBuilder.withPayload("bar".getBytes()).build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = module2InputChannel.receive(5000);
		assertNotNull(tapped);

		// Removed tap does not
		assertNull(module3InputChannel.receive(1000));

		// when other tap stream is deleted
		messageBus.unbindConsumer(fooTapName, module2InputChannel);
		// Clean up as StreamPlugin would
		messageBus.unbindConsumer("baz.0", moduleInputChannel);
		messageBus.unbindProducer("baz.0", moduleOutputChannel);
		messageBus.unbindProducers("tap:baz.http");
		assertTrue(getBindings(messageBus).isEmpty());
	}

}
