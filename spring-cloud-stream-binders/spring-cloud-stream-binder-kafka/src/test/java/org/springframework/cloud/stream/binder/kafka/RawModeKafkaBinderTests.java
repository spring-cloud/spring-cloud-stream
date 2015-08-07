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

package org.springframework.cloud.stream.binder.kafka;

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

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.channel.interceptor.WireTap;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.BinderProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.TestUtils;

/**
 * @author Marius Bogoevici
 * @author David Turanski
 */

public class RawModeKafkaBinderTests extends KafkaBinderTests {

	@Override
	protected KafkaTestBinder createKafkaTestBinder() {
		return new KafkaTestBinder(kafkaTestSupport, KafkaMessageChannelBinder.Mode.raw);
	}

	@Test
	@Override
	public void testPartitionedModuleJava() throws Exception {
		Binder binder = getBinder();
		Properties properties = new Properties();
		properties.put("partitionKeyExtractorClass", "org.springframework.cloud.stream.binder.kafka.RawKafkaPartitionTestSupport");
		properties.put("partitionSelectorClass", "org.springframework.cloud.stream.binder.kafka.RawKafkaPartitionTestSupport");
		properties.put(BinderProperties.NEXT_MODULE_COUNT, "3");
		properties.put(BinderProperties.NEXT_MODULE_CONCURRENCY, "2");

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		binder.bindProducer("partJ.0", output, properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(1, bindings.size());

		properties.clear();
		properties.put("concurrency", "2");
		properties.put("count","3");
		properties.put("partitionIndex", "0");
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0J");
		binder.bindConsumer("partJ.0", input0, properties);
		properties.put("partitionIndex", "1");
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1J");
		binder.bindConsumer("partJ.0", input1, properties);
		properties.put("partitionIndex", "2");
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2J");
		binder.bindConsumer("partJ.0", input2, properties);

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

		binder.unbindConsumers("partJ.0");
		binder.unbindProducers("partJ.0");
	}

	@Test
	@Override
	public void testPartitionedModuleSpEL() throws Exception {
		Binder binder = getBinder();
		Properties properties = new Properties();
		properties.put("partitionKeyExpression", "payload[0]");
		properties.put("partitionSelectorExpression", "hashCode()");
		properties.put(BinderProperties.NEXT_MODULE_COUNT, "3");
		properties.put(BinderProperties.NEXT_MODULE_CONCURRENCY, "2");

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		binder.bindProducer("part.0", output, properties);
		@SuppressWarnings("unchecked")
		List<Binding> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
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
		binder.bindConsumer("part.0", input0, properties);
		properties.put("partitionIndex", "1");
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1S");
		binder.bindConsumer("part.0", input1, properties);
		properties.put("partitionIndex", "2");
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2S");
		binder.bindConsumer("part.0", input2, properties);

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

		binder.unbindConsumers("part.0");
		binder.unbindProducers("part.0");
	}

	@Test
	@Override
	public void createInboundPubSubBeforeOutboundPubSub() throws Exception {
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		// Test pub/sub by emulating how StreamPlugin handles taps
		DirectChannel tapChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		// Create the tap first
		String fooTapName = binder.isCapable(Binder.Capability.DURABLE_PUBSUB) ? "foo.tap:baz.http" : "tap:baz.http";
		binder.bindPubSubConsumer(fooTapName, module2InputChannel, null);

		// Then create the stream
		binder.bindProducer("baz.0", moduleOutputChannel, null);
		binder.bindConsumer("baz.0", moduleInputChannel, null);
		moduleOutputChannel.addInterceptor(new WireTap(tapChannel));
		binder.bindPubSubProducer("tap:baz.http", tapChannel, null);

		// Another new module is using tap as an input channel
		String barTapName = binder.isCapable(Binder.Capability.DURABLE_PUBSUB) ? "bar.tap:baz.http" : "tap:baz.http";
		binder.bindPubSubConsumer(barTapName, module3InputChannel, null);
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
			assertNull(tapped1.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo", new String((byte[])tapped2.getPayload()));
		}
		// delete one tap stream is deleted
		binder.unbindConsumer(barTapName, module3InputChannel);
		Message<?> message2 = MessageBuilder.withPayload("bar".getBytes()).setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar").build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = module2InputChannel.receive(5000);
		assertNotNull(tapped);

		// Removed tap does not
		assertNull(module3InputChannel.receive(1000));

		// when other tap stream is deleted
		binder.unbindConsumer(fooTapName, module2InputChannel);
		// Clean up as StreamPlugin would
		binder.unbindConsumer("baz.0", moduleInputChannel);
		binder.unbindProducer("baz.0", moduleOutputChannel);
		binder.unbindProducers("tap:baz.http");
		assertTrue(getBindings(binder).isEmpty());
	}

	@Test
	@Override
	public void testSendAndReceive() throws Exception {
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		binder.bindProducer("foo.0", moduleOutputChannel, null);
		binder.bindConsumer("foo.0", moduleInputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo".getBytes()).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(5000);
		assertNotNull(inbound);
		assertEquals("foo", new String((byte[])inbound.getPayload()));
		binder.unbindProducers("foo.0");
		binder.unbindConsumers("foo.0");
	}

	// Ignored, since raw mode does not support headers
	@Test
	@Override
	@Ignore
	public void testSendAndReceiveNoOriginalContentType() throws Exception {

	}

	@Test
	public void testSendAndReceivePubSub() throws Exception {
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		// Test pub/sub by emulating how StreamPlugin handles taps
		DirectChannel tapChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		binder.bindProducer("baz.0", moduleOutputChannel, null);
		binder.bindConsumer("baz.0", moduleInputChannel, null);
		moduleOutputChannel.addInterceptor(new WireTap(tapChannel));
		binder.bindPubSubProducer("tap:baz.http", tapChannel, null);
		// A new module is using the tap as an input channel
		String fooTapName = binder.isCapable(Binder.Capability.DURABLE_PUBSUB) ? "foo.tap:baz.http" : "tap:baz.http";
		binder.bindPubSubConsumer(fooTapName, module2InputChannel, null);
		// Another new module is using tap as an input channel
		String barTapName = binder.isCapable(Binder.Capability.DURABLE_PUBSUB) ? "bar.tap:baz.http" : "tap:baz.http";
		binder.bindPubSubConsumer(barTapName, module3InputChannel, null);
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
		binder.unbindConsumer(barTapName, module3InputChannel);
		Message<?> message2 = MessageBuilder.withPayload("bar".getBytes()).build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = module2InputChannel.receive(5000);
		assertNotNull(tapped);

		// Removed tap does not
		assertNull(module3InputChannel.receive(1000));

		// when other tap stream is deleted
		binder.unbindConsumer(fooTapName, module2InputChannel);
		// Clean up as StreamPlugin would
		binder.unbindConsumer("baz.0", moduleInputChannel);
		binder.unbindProducer("baz.0", moduleOutputChannel);
		binder.unbindProducers("tap:baz.http");
		assertTrue(getBindings(binder).isEmpty());
	}

}
