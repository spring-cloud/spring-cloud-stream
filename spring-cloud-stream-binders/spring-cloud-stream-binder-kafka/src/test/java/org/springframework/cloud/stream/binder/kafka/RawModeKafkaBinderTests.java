/*
 * Copyright 2015-2016 the original author or authors.
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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderPropertyKeys;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.TestUtils;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Marius Bogoevici
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 */
public class RawModeKafkaBinderTests extends KafkaBinderTests {

	@Before
	public void init() {
		String multiplier = System.getenv("KAFKA_TIMEOUT_MULTIPLIER");
		if (multiplier != null) {
			timeoutMultiplier = Double.parseDouble(multiplier);
		}
	}

	@Override
	protected KafkaTestBinder createKafkaTestBinder() {
		return new KafkaTestBinder(kafkaTestSupport, KafkaMessageChannelBinder.Mode.raw);
	}

	@Test
	@Override
	public void testPartitionedModuleJava() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();
		properties.put("partitionKeyExtractorClass", "org.springframework.cloud.stream.binder.kafka.RawKafkaPartitionTestSupport");
		properties.put("partitionSelectorClass", "org.springframework.cloud.stream.binder.kafka.RawKafkaPartitionTestSupport");
		properties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "3");
		properties.put(BinderPropertyKeys.NEXT_MODULE_CONCURRENCY, "2");

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partJ.0", output, properties);
		@SuppressWarnings("unchecked")
		List<Binding<MessageChannel>> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
		assertEquals(1, bindings.size());

		properties.clear();
		properties.put("concurrency", "2");
		properties.put("count","3");
		properties.put("partitionIndex", "0");
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0J");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partJ.0", "test", input0, properties);
		properties.put("partitionIndex", "1");
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1J");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partJ.0", "test", input1, properties);
		properties.put("partitionIndex", "2");
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2J");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("partJ.0", "test", input2, properties);

		output.send(new GenericMessage<>(new byte[]{(byte)0}));
		output.send(new GenericMessage<>(new byte[]{(byte)1}));
		output.send(new GenericMessage<>(new byte[]{(byte)2}));

		Message<?> receive0 = receive(input0);
		assertNotNull(receive0);
		Message<?> receive1 = receive(input1);
		assertNotNull(receive1);
		Message<?> receive2 = receive(input2);
		assertNotNull(receive2);

		assertThat(Arrays.asList(
						((byte[]) receive0.getPayload())[0],
						((byte[]) receive1.getPayload())[0],
						((byte[]) receive2.getPayload())[0]),
				containsInAnyOrder((byte)0, (byte)1, (byte)2));

		binder.unbind(input0Binding);
		binder.unbind(input1Binding);
		binder.unbind(input2Binding);
		binder.unbind(outputBinding);
	}

	@Test
	@Override
	public void testPartitionedModuleSpEL() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Properties properties = new Properties();
		properties.put("partitionKeyExpression", "payload[0]");
		properties.put("partitionSelectorExpression", "hashCode()");
		properties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "3");
		properties.put(BinderPropertyKeys.NEXT_MODULE_CONCURRENCY, "2");

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("part.0", output, properties);
		@SuppressWarnings("unchecked")
		List<Binding<MessageChannel>> bindings = TestUtils.getPropertyValue(binder, "binder.bindings", List.class);
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
		Binding<MessageChannel> input0Binding = binder.bindConsumer("part.0", "test", input0, properties);
		properties.put("partitionIndex", "1");
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1S");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("part.0", "test", input1, properties);
		properties.put("partitionIndex", "2");
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2S");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("part.0", "test", input2, properties);

		Message<byte[]> message2 = MessageBuilder.withPayload(new byte[]{2})
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "foo")
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 42)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 43)
				.setHeader("xdReplyChannel", "bar")
				.build();
		output.send(message2);
		output.send(new GenericMessage<>(new byte[]{1}));
		output.send(new GenericMessage<>(new byte[]{0}));

		Message<?> receive0 = receive(input0);
		assertNotNull(receive0);
		Message<?> receive1 = receive(input1);
		assertNotNull(receive1);
		Message<?> receive2 = receive(input2);
		assertNotNull(receive2);


		assertThat(Arrays.asList(
						((byte[]) receive0.getPayload())[0],
						((byte[]) receive1.getPayload())[0],
						((byte[]) receive2.getPayload())[0]),
				containsInAnyOrder((byte)0, (byte)1, (byte)2));

		binder.unbind(input0Binding);
		binder.unbind(input1Binding);
		binder.unbind(input2Binding);
		binder.unbind(outputBinding);
	}

	@Test
	@Override
	public void testSendAndReceive() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel, null);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "test", moduleInputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo".getBytes()).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertNotNull(inbound);
		assertEquals("foo", new String((byte[])inbound.getPayload()));
		binder.unbind(producerBinding);
		binder.unbind(consumerBinding);
	}

	// Ignored, since raw mode does not support headers
	@Test
	@Override
	@Ignore
	public void testSendAndReceiveNoOriginalContentType() throws Exception {

	}

	@Test
	public void testSendAndReceiveWithExplicitConsumerGroup() {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		// Test pub/sub by emulating how StreamPlugin handles taps
		QueueChannel module1InputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		Binding<MessageChannel> producerBinding = binder.bindProducer("baz.0", moduleOutputChannel, null);
		Binding<MessageChannel> input1Binding = binder.bindConsumer("baz.0", "test", module1InputChannel, null);
		// A new module is using the tap as an input channel
		String fooTapName = "baz.0";
		Binding<MessageChannel> input2Binding = binder.bindConsumer(fooTapName, "tap1", module2InputChannel, null);
		// Another new module is using tap as an input channel
		String barTapName = "baz.0";
		Binding<MessageChannel> input3Binding = binder.bindConsumer(barTapName, "tap2", module3InputChannel, null);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes()).build();
		boolean success = false;
		boolean retried = false;
		while (!success) {
			moduleOutputChannel.send(message);
			Message<?> inbound = receive(module1InputChannel);
			assertNotNull(inbound);
			assertEquals("foo", new String((byte[])inbound.getPayload()));

			Message<?> tapped1 = receive(module2InputChannel);
			Message<?> tapped2 = receive(module3InputChannel);
			if (tapped1 == null || tapped2 == null) {
				// listener may not have started
				assertFalse("Failed to receive tap after retry", retried);
				retried = true;
				continue;
			}
			success = true;
			assertEquals("foo", new String((byte[]) tapped1.getPayload()));
			assertEquals("foo", new String((byte[]) tapped2.getPayload()));
		}
		// delete one tap stream is deleted
		binder.unbind(input3Binding);
		Message<?> message2 = MessageBuilder.withPayload("bar".getBytes()).build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = receive(module2InputChannel);
		assertNotNull(tapped);

		// removed tap does not
		assertNull(receive(module3InputChannel));

		// re-subscribed tap does receive the message
		input3Binding = binder.bindConsumer(barTapName, "tap2", module3InputChannel, null);
		assertNotNull(receive(module3InputChannel));

		// clean up
		binder.unbind(input1Binding);
		binder.unbind(input2Binding);
		binder.unbind(input3Binding);
		binder.unbind(producerBinding);
		assertTrue(getBindings(binder).isEmpty());
	}

}
