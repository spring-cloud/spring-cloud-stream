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

import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 */
public class RawModeKafkaBinderTests extends KafkaBinderTests {

	@Test
	@Override
	public void testPartitionedModuleJava() throws Exception {
		KafkaTestBinder binder = getBinder();
		ExtendedProducerProperties<KafkaProducerProperties> properties = createProducerProperties();
		properties.setHeaderMode(HeaderMode.raw);
		properties.setPartitionKeyExtractorClass(RawKafkaPartitionTestSupport.class);
		properties.setPartitionSelectorClass(RawKafkaPartitionTestSupport.class);
		properties.setPartitionCount(6);

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partJ.0", output, properties);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setPartitioned(true);
		consumerProperties.setHeaderMode(HeaderMode.raw);
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0J");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partJ.0", "test", input0, consumerProperties);
		consumerProperties.setInstanceIndex(1);
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1J");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partJ.0", "test", input1, consumerProperties);
		consumerProperties.setInstanceIndex(2);
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2J");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("partJ.0", "test", input2, consumerProperties);

		output.send(new GenericMessage<>(new byte[] { (byte) 0 }));
		output.send(new GenericMessage<>(new byte[] { (byte) 1 }));
		output.send(new GenericMessage<>(new byte[] { (byte) 2 }));

		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();

		assertThat(Arrays.asList(((byte[]) receive0.getPayload())[0], ((byte[]) receive1.getPayload())[0],
				((byte[]) receive2.getPayload())[0])).containsExactlyInAnyOrder((byte) 0, (byte) 1, (byte) 2);

		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	@Test
	@Override
	public void testPartitionedModuleSpEL() throws Exception {
		KafkaTestBinder binder = getBinder();
		ExtendedProducerProperties<KafkaProducerProperties> properties = createProducerProperties();
		properties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload[0]"));
		properties.setPartitionSelectorExpression(spelExpressionParser.parseExpression("hashCode()"));
		properties.setPartitionCount(6);
		properties.setHeaderMode(HeaderMode.raw);

		DirectChannel output = new DirectChannel();
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("part.0", output, properties);
		try {
			AbstractEndpoint endpoint = extractEndpoint(outputBinding);
			assertThat(getEndpointRouting(endpoint)).contains("part.0-' + headers['partition']");
		}
		catch (UnsupportedOperationException ignored) {

		}

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setPartitioned(true);
		consumerProperties.setHeaderMode(HeaderMode.raw);
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0S");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("part.0", "test", input0, consumerProperties);
		consumerProperties.setInstanceIndex(1);
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1S");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("part.0", "test", input1, consumerProperties);
		consumerProperties.setInstanceIndex(2);
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2S");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("part.0", "test", input2, consumerProperties);

		Message<byte[]> message2 = MessageBuilder.withPayload(new byte[] { 2 })
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "foo")
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 42)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 43).build();
		output.send(message2);
		output.send(new GenericMessage<>(new byte[] { 1 }));
		output.send(new GenericMessage<>(new byte[] { 0 }));
		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();
		assertThat(Arrays.asList(((byte[]) receive0.getPayload())[0], ((byte[]) receive1.getPayload())[0],
				((byte[]) receive2.getPayload())[0])).containsExactlyInAnyOrder((byte) 0, (byte) 1, (byte) 2);
		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	@Test
	@Override
	public void testSendAndReceive() throws Exception {
		KafkaTestBinder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setHeaderMode(HeaderMode.raw);
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel, producerProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setHeaderMode(HeaderMode.raw);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "test", moduleInputChannel,
				consumerProperties);
		Message<?> message = MessageBuilder.withPayload("foo".getBytes()).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat(new String((byte[]) inbound.getPayload())).isEqualTo("foo");
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	// Ignored, since raw mode does not support headers
	@Test
	@Override
	@Ignore
	public void testSendAndReceiveNoOriginalContentType() throws Exception {

	}

	@Test
	public void testSendAndReceiveWithExplicitConsumerGroup() {
		KafkaTestBinder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		// Test pub/sub by emulating how StreamPlugin handles taps
		QueueChannel module1InputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setHeaderMode(HeaderMode.raw);
		Binding<MessageChannel> producerBinding = binder.bindProducer("baz.0", moduleOutputChannel, producerProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setHeaderMode(HeaderMode.raw);
		Binding<MessageChannel> input1Binding = binder.bindConsumer("baz.0", "test", module1InputChannel,
				consumerProperties);
		// A new module is using the tap as an input channel
		String fooTapName = "baz.0";
		Binding<MessageChannel> input2Binding = binder.bindConsumer(fooTapName, "tap1", module2InputChannel,
				consumerProperties);
		// Another new module is using tap as an input channel
		String barTapName = "baz.0";
		Binding<MessageChannel> input3Binding = binder.bindConsumer(barTapName, "tap2", module3InputChannel,
				consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes()).build();
		boolean success = false;
		boolean retried = false;
		while (!success) {
			moduleOutputChannel.send(message);
			Message<?> inbound = receive(module1InputChannel);
			assertThat(inbound).isNotNull();
			assertThat(new String((byte[]) inbound.getPayload())).isEqualTo("foo");

			Message<?> tapped1 = receive(module2InputChannel);
			Message<?> tapped2 = receive(module3InputChannel);
			if (tapped1 == null || tapped2 == null) {
				// listener may not have started
				assertThat(retried).isFalse().withFailMessage("Failed to receive tap after retry");
				retried = true;
				continue;
			}
			success = true;
			assertThat(new String((byte[]) tapped1.getPayload())).isEqualTo("foo");
			assertThat(new String((byte[]) tapped2.getPayload())).isEqualTo("foo");
		}
		// delete one tap stream is deleted
		input3Binding.unbind();
		Message<?> message2 = MessageBuilder.withPayload("bar".getBytes()).build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = receive(module2InputChannel);
		assertThat(tapped).isNotNull();

		// removed tap does not
		assertThat(receive(module3InputChannel)).isNull();

		// re-subscribed tap does receive the message
		input3Binding = binder.bindConsumer(barTapName, "tap2", module3InputChannel, createConsumerProperties());
		assertThat(receive(module3InputChannel)).isNotNull();

		// clean up
		input1Binding.unbind();
		input2Binding.unbind();
		input3Binding.unbind();
		producerBinding.unbind();
		assertThat(extractEndpoint(input1Binding).isRunning()).isFalse();
		assertThat(extractEndpoint(input2Binding).isRunning()).isFalse();
		assertThat(extractEndpoint(input3Binding).isRunning()).isFalse();
		assertThat(extractEndpoint(producerBinding).isRunning()).isFalse();
	}
}
