/*
 * Copyright 2014-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.assertj.core.api.Condition;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.context.Lifecycle;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for binders that support partitioning.
 *
 * @author Gary Russell
 * @author Mark Fisher
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 */
// @checkstyle:off
public abstract class PartitionCapableBinderTests<B extends AbstractTestBinder<? extends AbstractBinder<MessageChannel, CP, PP>, CP, PP>, CP extends ConsumerProperties, PP extends ProducerProperties>
		extends AbstractBinderTests<B, CP, PP> {

	// @checkstyle:on

	protected static final SpelExpressionParser spelExpressionParser = new SpelExpressionParser();

	@Test
	@SuppressWarnings("unchecked")
	public void testAnonymousGroup() throws Exception {
		B binder = getBinder();
		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties());
		DirectChannel output = createBindableChannel("output", producerBindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer(
				String.format("defaultGroup%s0", getDestinationNameDelimiter()), output,
				(PP) producerBindingProperties.getProducer());

		QueueChannel input1 = new QueueChannel();
		Binding<MessageChannel> binding1 = binder.bindConsumer(
				String.format("defaultGroup%s0", getDestinationNameDelimiter()), null,
				input1, createConsumerProperties());

		QueueChannel input2 = new QueueChannel();
		Binding<MessageChannel> binding2 = binder.bindConsumer(
				String.format("defaultGroup%s0", getDestinationNameDelimiter()), null,
				input2, createConsumerProperties());

		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload1)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());

		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload1);

		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload1);

		binding2.unbind();

		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload2)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());

		binding2 = binder.bindConsumer(
				String.format("defaultGroup%s0", getDestinationNameDelimiter()), null,
				input2, createConsumerProperties());
		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload3)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());

		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload2);
		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isNotNull();

		receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload3);

		producerBinding.unbind();
		binding1.unbind();
		binding2.unbind();
	}

	@Test
	public void testOneRequiredGroup() throws Exception {
		B binder = getBinder();
		PP producerProperties = createProducerProperties();
		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));

		String testDestination = "testDestination"
				+ UUID.randomUUID().toString().replace("-", "");

		producerProperties.setRequiredGroups("test1");
		Binding<MessageChannel> producerBinding = binder.bindProducer(testDestination,
				output, producerProperties);

		String testPayload = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());

		QueueChannel inbound1 = new QueueChannel();
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testDestination,
				"test1", inbound1, createConsumerProperties());

		Message<?> receivedMessage1 = receive(inbound1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String((byte[]) receivedMessage1.getPayload()))
				.isEqualTo(testPayload);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testTwoRequiredGroups() throws Exception {
		B binder = getBinder();
		PP producerProperties = createProducerProperties();

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));

		String testDestination = "testDestination"
				+ UUID.randomUUID().toString().replace("-", "");

		producerProperties.setRequiredGroups("test1", "test2");
		Binding<MessageChannel> producerBinding = binder.bindProducer(testDestination,
				output, producerProperties);

		String testPayload = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());

		QueueChannel inbound1 = new QueueChannel();
		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer(testDestination,
				"test1", inbound1, createConsumerProperties());
		QueueChannel inbound2 = new QueueChannel();
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer(testDestination,
				"test2", inbound2, createConsumerProperties());

		Message<?> receivedMessage1 = receive(inbound1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String((byte[]) receivedMessage1.getPayload()))
				.isEqualTo(testPayload);
		Message<?> receivedMessage2 = receive(inbound2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String((byte[]) receivedMessage2.getPayload()))
				.isEqualTo(testPayload);

		consumerBinding1.unbind();
		consumerBinding2.unbind();
		producerBinding.unbind();
	}

	@Test
	public void testPartitionedModuleSpEL() throws Exception {
		B binder = getBinder();

		CP consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setPartitioned(true);
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0S");
		Binding<MessageChannel> input0Binding = binder.bindConsumer(
				String.format("part%s0", getDestinationNameDelimiter()),
				"testPartitionedModuleSpEL", input0, consumerProperties);
		consumerProperties.setInstanceIndex(1);
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1S");
		Binding<MessageChannel> input1Binding = binder.bindConsumer(
				String.format("part%s0", getDestinationNameDelimiter()),
				"testPartitionedModuleSpEL", input1, consumerProperties);
		consumerProperties.setInstanceIndex(2);
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2S");
		Binding<MessageChannel> input2Binding = binder.bindConsumer(
				String.format("part%s0", getDestinationNameDelimiter()),
				"testPartitionedModuleSpEL", input2, consumerProperties);

		PP producerProperties = createProducerProperties();
		producerProperties.setPartitionKeyExpression(
				spelExpressionParser.parseExpression("payload"));
		producerProperties.setPartitionSelectorExpression(
				spelExpressionParser.parseExpression("hashCode()"));
		producerProperties.setPartitionCount(3);

		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer(
				String.format("part%s0", getDestinationNameDelimiter()), output,
				producerProperties);
		try {
			Object endpoint = extractEndpoint(outputBinding);
			checkRkExpressionForPartitionedModuleSpEL(endpoint);
		}
		catch (UnsupportedOperationException ignored) {
		}

		Message<String> message2 = MessageBuilder.withPayload("2")
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "foo")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 42)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 43).build();
		output.send(message2);
		output.send(MessageBuilder.withPayload("1")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());
		output.send(MessageBuilder.withPayload("0")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());

		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();

		Condition<Message<?>> correlationHeadersForPayload2 = new Condition<Message<?>>() {
			@Override
			public boolean matches(Message<?> value) {
				IntegrationMessageHeaderAccessor accessor = new IntegrationMessageHeaderAccessor(
						value);
				return "foo".equals(accessor.getCorrelationId())
						&& 42 == accessor.getSequenceNumber()
						&& 43 == accessor.getSequenceSize();
			}
		};

		if (usesExplicitRouting()) {
			assertThat(receive0.getPayload()).isEqualTo("0".getBytes());
			assertThat(receive1.getPayload()).isEqualTo("1".getBytes());
			assertThat(receive2.getPayload()).isEqualTo("2".getBytes());
			assertThat(receive2).has(correlationHeadersForPayload2);
		}
		else {
			List<Message<?>> receivedMessages = Arrays.asList(receive0, receive1,
					receive2);
			assertThat(receivedMessages).extracting("payload").containsExactlyInAnyOrder(
					"0".getBytes(), "1".getBytes(), "2".getBytes());
			Condition<Message<?>> payloadIs2 = new Condition<Message<?>>() {

				@Override
				public boolean matches(Message<?> value) {
					return value.getPayload().equals("2".getBytes());
				}
			};
			assertThat(receivedMessages).filteredOn(payloadIs2).areExactly(1,
					correlationHeadersForPayload2);

		}
		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	protected void checkRkExpressionForPartitionedModuleSpEL(Object endpoint) {
		assertThat(getEndpointRouting(endpoint))
				.contains(getExpectedRoutingBaseDestination(
						String.format("part%s0", getDestinationNameDelimiter()), "test")
						+ "-' + headers['partition']");
	}

	@Test
	@Ignore
	public void testPartitionedModuleJava() throws Exception {
		B binder = getBinder();

		CP consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setPartitioned(true);
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0J");
		Binding<MessageChannel> input0Binding = binder.bindConsumer(
				String.format("partJ%s0", getDestinationNameDelimiter()),
				"testPartitionedModuleJava", input0, consumerProperties);
		consumerProperties.setInstanceIndex(1);
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1J");
		Binding<MessageChannel> input1Binding = binder.bindConsumer(
				String.format("partJ%s0", getDestinationNameDelimiter()),
				"testPartitionedModuleJava", input1, consumerProperties);
		consumerProperties.setInstanceIndex(2);
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2J");
		Binding<MessageChannel> input2Binding = binder.bindConsumer(
				String.format("partJ%s0", getDestinationNameDelimiter()),
				"testPartitionedModuleJava", input2, consumerProperties);

		PP producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(3);
		DirectChannel output = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partJ.0", output,
				producerProperties);
		if (usesExplicitRouting()) {
			Object endpoint = extractEndpoint(outputBinding);
			assertThat(getEndpointRouting(endpoint))
					.contains(getExpectedRoutingBaseDestination(
							String.format("partJ%s0", getDestinationNameDelimiter()),
							"testPartitionedModuleJava") + "-' + headers['"
							+ BinderHeaders.PARTITION_HEADER + "']");
		}

		output.send(MessageBuilder.withPayload("2")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());
		output.send(MessageBuilder.withPayload("1")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());
		output.send(MessageBuilder.withPayload("0")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.build());

		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();

		if (usesExplicitRouting()) {
			assertThat(receive0.getPayload()).isEqualTo("0".getBytes());
			assertThat(receive1.getPayload()).isEqualTo("1".getBytes());
			assertThat(receive2.getPayload()).isEqualTo("2".getBytes());
		}
		else {
			List<Message<?>> receivedMessages = Arrays.asList(receive0, receive1,
					receive2);
			assertThat(receivedMessages).extracting("payload").containsExactlyInAnyOrder(
					"0".getBytes(), "1".getBytes(), "2".getBytes());
		}

		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	/**
	 * Implementations should return whether the binder under test uses "explicit" routing
	 * (e.g. Rabbit) whereby Spring Cloud Stream is responsible for assigning a partition
	 * and knows which exact consumer will receive the message (i.e. honor
	 * "partitionIndex") or "implicit" routing (e.g. Kafka) whereby the only guarantee is
	 * that messages will be spread, but we don't control exactly which consumer gets
	 * which message.
	 */
	protected abstract boolean usesExplicitRouting();

	/**
	 * For implementations that rely on explicit routing, return the routing expression.
	 */
	protected String getEndpointRouting(Object endpoint) {
		throw new UnsupportedOperationException();
	}

	/**
	 * For implementations that rely on explicit routing, return the expected base
	 * destination (the part that precedes '-partition' within the expression).
	 */
	protected String getExpectedRoutingBaseDestination(String name, String group) {
		throw new UnsupportedOperationException();
	}

	protected abstract String getClassUnderTestName();

	protected Lifecycle extractEndpoint(Binding<MessageChannel> binding) {
		DirectFieldAccessor accessor = new DirectFieldAccessor(binding);
		return (Lifecycle) accessor.getPropertyValue("lifecycle");
	}

}
