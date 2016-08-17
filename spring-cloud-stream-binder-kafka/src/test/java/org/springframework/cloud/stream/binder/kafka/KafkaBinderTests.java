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

package org.springframework.cloud.stream.binder.kafka;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.assertj.core.api.Condition;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PartitionTestSupport;
import org.springframework.cloud.stream.binder.TestUtils;
import org.springframework.cloud.stream.binder.kafka.configuration.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Soby Chacko
 */
public abstract class KafkaBinderTests extends PartitionCapableBinderTests<AbstractKafkaTestBinder, ExtendedConsumerProperties<KafkaConsumerProperties>,
						ExtendedProducerProperties<KafkaProducerProperties>> {

	protected abstract ExtendedConsumerProperties<KafkaConsumerProperties> createConsumerProperties();

	protected abstract ExtendedProducerProperties<KafkaProducerProperties> createProducerProperties();

	public abstract String getKafkaOffsetHeaderKey();

	protected abstract Binder getBinder(KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties);

	protected abstract KafkaBinderConfigurationProperties createConfigurationProperties();

	protected abstract int partitionSize(String topic);

	protected abstract void setMetadataRetryOperations(Binder binder, RetryOperations retryOperations);

	protected abstract ZkUtils getZkUtils(KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties);

	protected abstract void invokeCreateTopic(ZkUtils zkUtils, String topic, int partitions,
												int replicationFactor, Properties topicConfig);

	protected abstract int invokePartitionSize(String topic,
												ZkUtils zkUtils);

	@Test
	@SuppressWarnings("unchecked")
	public void testDlqAndRetry() throws Exception {
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		QueueChannel dlqChannel = new QueueChannel();
		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(2);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setEnableDlq(true);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		long uniqueBindingId = System.currentTimeMillis();

		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0",
				"testGroup", moduleInputChannel, consumerProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);

		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(
				"error.retryTest." + uniqueBindingId + ".0.testGroup", null, dlqChannel, dlqConsumerProperties);
		binderBindUnbindLatency();
		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage = MessageBuilder.withPayload(testMessagePayload).build();
		moduleOutputChannel.send(testMessage);

		Message<?> receivedMessage = receive(dlqChannel, 3);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo(testMessagePayload);
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
		binderBindUnbindLatency();
		dlqConsumerBinding.unbind();
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDefaultAutoCommitOnErrorWithoutDlq() throws Exception {
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(1);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0",
				"testGroup", moduleInputChannel, consumerProperties);

		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage = MessageBuilder.withPayload(testMessagePayload).build();
		moduleOutputChannel.send(testMessage);

		assertThat(handler.getLatch().await((int) (timeoutMultiplier * 1000), TimeUnit.MILLISECONDS));
		// first attempt fails
		assertThat(handler.getReceivedMessages().entrySet()).hasSize(1);
		Message<?> receivedMessage = handler.getReceivedMessages().entrySet().iterator().next().getValue();
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo(testMessagePayload);
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
		consumerBinding.unbind();

		// on the second attempt the message is redelivered
		QueueChannel successfulInputChannel = new QueueChannel();
		consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0", "testGroup",
				successfulInputChannel, consumerProperties);
		binderBindUnbindLatency();
		String testMessage2Payload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage2 = MessageBuilder.withPayload(testMessage2Payload).build();
		moduleOutputChannel.send(testMessage2);

		Message<?> firstReceived = receive(successfulInputChannel);
		assertThat(firstReceived.getPayload()).isEqualTo(testMessagePayload);
		Message<?> secondReceived = receive(successfulInputChannel);
		assertThat(secondReceived.getPayload()).isEqualTo(testMessage2Payload);
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDefaultAutoCommitOnErrorWithDlq() throws Exception {
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setEnableDlq(true);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0",
				"testGroup", moduleInputChannel, consumerProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);
		QueueChannel dlqChannel = new QueueChannel();
		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(
				"error.retryTest." + uniqueBindingId + ".0.testGroup", null, dlqChannel, dlqConsumerProperties);

		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage = MessageBuilder.withPayload(testMessagePayload).build();
		moduleOutputChannel.send(testMessage);

		Message<?> dlqMessage = receive(dlqChannel, 3);
		assertThat(dlqMessage).isNotNull();
		assertThat(dlqMessage.getPayload()).isEqualTo(testMessagePayload);

		// first attempt fails
		assertThat(handler.getReceivedMessages().entrySet()).hasSize(1);
		Message<?> handledMessage = handler.getReceivedMessages().entrySet().iterator().next().getValue();
		assertThat(handledMessage).isNotNull();
		assertThat(handledMessage.getPayload()).isEqualTo(testMessagePayload);
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
		binderBindUnbindLatency();
		dlqConsumerBinding.unbind();
		consumerBinding.unbind();

		// on the second attempt the message is not redelivered because the DLQ is set
		QueueChannel successfulInputChannel = new QueueChannel();
		consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0", "testGroup",
				successfulInputChannel, consumerProperties);
		String testMessage2Payload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage2 = MessageBuilder.withPayload(testMessage2Payload).build();
		moduleOutputChannel.send(testMessage2);

		Message<?> receivedMessage = receive(successfulInputChannel);
		assertThat(receivedMessage.getPayload()).isEqualTo(testMessage2Payload);

		binderBindUnbindLatency();
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoCreateTopicsEnabledSucceeds() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		configurationProperties.setAutoCreateTopics(true);
		Binder binder = getBinder(configurationProperties);
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		setMetadataRetryOperations(binder, metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		String testTopicName = "nonexisting" + System.currentTimeMillis();
		Binding<?> binding = binder.bindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidateKafkaTopicName() {
		KafkaTopicUtils.validateTopicName("foo:bar");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCompression() throws Exception {
		final KafkaProducerProperties.CompressionType[] codecs = new KafkaProducerProperties.CompressionType[]{
				KafkaProducerProperties.CompressionType.none, KafkaProducerProperties.CompressionType.gzip,
				KafkaProducerProperties.CompressionType.snappy};
		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		Binder binder = getBinder();
		for (KafkaProducerProperties.CompressionType codec : codecs) {
			DirectChannel moduleOutputChannel = new DirectChannel();
			QueueChannel moduleInputChannel = new QueueChannel();
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
			producerProperties.getExtension().setCompressionType(
					KafkaProducerProperties.CompressionType.valueOf(codec.toString()));
			Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel,
					producerProperties);
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			consumerProperties.getExtension().setAutoRebalanceEnabled(false);
			Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "test", moduleInputChannel,
					consumerProperties);
			Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
					.build();
			// Let the consumer actually bind to the producer before sending a msg
			binderBindUnbindLatency();
			moduleOutputChannel.send(message);
			Message<?> inbound = receive(moduleInputChannel);
			assertThat(inbound).isNotNull();
			assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);
			producerBinding.unbind();
			consumerBinding.unbind();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCustomPartitionCountOverridesDefaultIfLarger() throws Exception {

		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setMinPartitionCount(10);
		Binder binder = getBinder(binderConfiguration);
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);

		assertThat(partitionSize("foo" + uniqueBindingId + ".0")).isEqualTo(10);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCustomPartitionCountDoesNotOverridePartitioningIfSmaller() throws Exception {

		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setMinPartitionCount(6);
		Binder binder = getBinder(binderConfiguration);
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(5);
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Thread.sleep(1000);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);

		assertThat(partitionSize("foo" + uniqueBindingId + ".0")).isEqualTo(6);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCustomPartitionCountOverridesPartitioningIfLarger() throws Exception {

		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setMinPartitionCount(4);
		Binder binder = getBinder(binderConfiguration);

		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(5);
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);
		assertThat(partitionSize("foo" + uniqueBindingId + ".0")).isEqualTo(5);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDefaultConsumerStartsAtEarliest() throws Exception {
		Binder binder = getBinder(createConfigurationProperties());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		//binder.setApplicationContext(context);
		//binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output,
				createProducerProperties());
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1,
				consumerProperties);
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload1);
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload2);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEarliest() throws Exception {
		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			Binder binder = getBinder();
			DirectChannel output = new DirectChannel();
			QueueChannel input1 = new QueueChannel();

			String testTopicName = UUID.randomUUID().toString();
			producerBinding = binder.bindProducer(testTopicName, output, createProducerProperties());
			String testPayload1 = "foo-" + UUID.randomUUID().toString();
			output.send(new GenericMessage<>(testPayload1.getBytes()));
			ExtendedConsumerProperties<KafkaConsumerProperties> properties = createConsumerProperties();
			properties.getExtension().setAutoRebalanceEnabled(false);
			properties.getExtension().setStartOffset(KafkaConsumerProperties.StartOffset.earliest);
			consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1, properties);
			Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
			assertThat(receivedMessage1).isNotNull();
			String testPayload2 = "foo-" + UUID.randomUUID().toString();
			output.send(new GenericMessage<>(testPayload2.getBytes()));
			Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
			assertThat(receivedMessage2).isNotNull();
			assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload2);
			Thread.sleep(2000);
			producerBinding.unbind();
			consumerBinding.unbind();
		}
		finally {
			if (consumerBinding != null) {
				consumerBinding.unbind();
			}
			if (producerBinding != null) {
				producerBinding.unbind();
			}
		}
	}

	@Test
	@Ignore("Needs further discussion")
	@SuppressWarnings("unchecked")
	public void testReset() throws Exception {
		Binder binder = getBinder();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();

		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output,
				createProducerProperties());
		String testPayload1 = "foo1-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		ExtendedConsumerProperties<KafkaConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setResetOffsets(true);
		properties.getExtension().setStartOffset(KafkaConsumerProperties.StartOffset.earliest);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1,
				properties);
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload1);
		String testPayload2 = "foo2-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload2);
		consumerBinding.unbind();

		String testPayload3 = "foo3-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload3.getBytes()));

		ExtendedConsumerProperties<KafkaConsumerProperties> properties2 = createConsumerProperties();
		properties2.getExtension().setResetOffsets(true);
		properties2.getExtension().setStartOffset(KafkaConsumerProperties.StartOffset.earliest);
		consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1, properties2);
		Message<byte[]> receivedMessage4 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage4).isNotNull();
		assertThat(new String(receivedMessage4.getPayload())).isEqualTo(testPayload1);
		Message<byte[]> receivedMessage5 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage5).isNotNull();
		assertThat(new String(receivedMessage5.getPayload())).isEqualTo(testPayload2);
		Message<byte[]> receivedMessage6 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage6).isNotNull();
		assertThat(new String(receivedMessage6.getPayload())).isEqualTo(testPayload3);
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testResume() throws Exception {
		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
			Binder binder = getBinder(configurationProperties);
			DirectChannel output = new DirectChannel();
			QueueChannel input1 = new QueueChannel();

			String testTopicName = UUID.randomUUID().toString();
			producerBinding = binder.bindProducer(testTopicName, output,
					createProducerProperties());
			String testPayload1 = "foo1-" + UUID.randomUUID().toString();
			output.send(new GenericMessage<>(testPayload1.getBytes()));
			ExtendedConsumerProperties<KafkaConsumerProperties> firstConsumerProperties = createConsumerProperties();
			firstConsumerProperties.getExtension().setAutoRebalanceEnabled(false);
			consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1,
					firstConsumerProperties);
			Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
			assertThat(receivedMessage1).isNotNull();
			String testPayload2 = "foo2-" + UUID.randomUUID().toString();
			output.send(new GenericMessage<>(testPayload2.getBytes()));
			Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
			assertThat(receivedMessage2).isNotNull();
			assertThat(new String(receivedMessage2.getPayload())).isNotNull();
			consumerBinding.unbind();

			String testPayload3 = "foo3-" + UUID.randomUUID().toString();
			output.send(new GenericMessage<>(testPayload3.getBytes()));

			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1, consumerProperties);
			consumerProperties.getExtension().setAutoRebalanceEnabled(false);
			Message<byte[]> receivedMessage3 = (Message<byte[]>) receive(input1);
			assertThat(receivedMessage3).isNotNull();
			assertThat(new String(receivedMessage3.getPayload())).isEqualTo(testPayload3);

			Thread.sleep(2000);
		}
		finally {
			if (consumerBinding != null) {
				consumerBinding.unbind();
			}
			if (producerBinding != null) {
				producerBinding.unbind();
			}
		}
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testSendAndReceiveMultipleTopics() throws Exception {
		Binder binder = getBinder();

		DirectChannel moduleOutputChannel1 = createBindableChannel("output1",
				createProducerBindingProperties(createProducerProperties()));
		DirectChannel moduleOutputChannel2 = createBindableChannel("output2",
				createProducerBindingProperties(createProducerProperties()));

		QueueChannel moduleInputChannel = new QueueChannel();

		Binding<MessageChannel> producerBinding1 = binder.bindProducer("foo.x", moduleOutputChannel1,
				createProducerProperties());
		Binding<MessageChannel> producerBinding2 = binder.bindProducer("foo.y", moduleOutputChannel2,
				createProducerProperties());

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer("foo.x", "test", moduleInputChannel,
				consumerProperties);
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer("foo.y", "test", moduleInputChannel,
				consumerProperties);

		String testPayload1 = "foo" + UUID.randomUUID().toString();
		Message<?> message1 = org.springframework.integration.support.MessageBuilder.withPayload(
				testPayload1.getBytes()).build();
		String testPayload2 = "foo" + UUID.randomUUID().toString();
		Message<?> message2 = org.springframework.integration.support.MessageBuilder.withPayload(
				testPayload2.getBytes()).build();

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel1.send(message1);
		moduleOutputChannel2.send(message2);


		Message<?>[] messages = new Message[2];
		messages[0] = receive(moduleInputChannel);
		messages[1] = receive(moduleInputChannel);

		assertThat(messages[0]).isNotNull();
		assertThat(messages[1]).isNotNull();
		assertThat(messages).extracting("payload").containsExactlyInAnyOrder(testPayload1.getBytes(),
				testPayload2.getBytes());

		producerBinding1.unbind();
		producerBinding2.unbind();

		consumerBinding1.unbind();
		consumerBinding2.unbind();
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testTwoRequiredGroups() throws Exception {
		Binder binder = getBinder();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();

		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(producerProperties));

		String testDestination = "testDestination" + UUID.randomUUID().toString().replace("-", "");

		producerProperties.setRequiredGroups("test1", "test2");
		Binding<MessageChannel> producerBinding = binder.bindProducer(testDestination, output, producerProperties);

		String testPayload = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload.getBytes()));

		QueueChannel inbound1 = new QueueChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer(testDestination, "test1", inbound1,
				consumerProperties);
		QueueChannel inbound2 = new QueueChannel();
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer(testDestination, "test2", inbound2,
				consumerProperties);

		Message<?> receivedMessage1 = receive(inbound1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String((byte[]) receivedMessage1.getPayload())).isEqualTo(testPayload);
		Message<?> receivedMessage2 = receive(inbound2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String((byte[]) receivedMessage2.getPayload())).isEqualTo(testPayload);

		consumerBinding1.unbind();
		consumerBinding2.unbind();
		producerBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testPartitionedModuleSpEL() throws Exception {
		Binder binder = getBinder();

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setPartitioned(true);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
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

		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		producerProperties.setPartitionSelectorExpression(spelExpressionParser.parseExpression("hashCode()"));
		producerProperties.setPartitionCount(3);

		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(producerProperties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("part.0", output, producerProperties);
		try {
			Object endpoint = extractEndpoint(outputBinding);
			assertThat(getEndpointRouting(endpoint))
					.contains(getExpectedRoutingBaseDestination("part.0", "test") + "-' + headers['partition']");
		}
		catch (UnsupportedOperationException ignored) {
		}

		Message<Integer> message2 = org.springframework.integration.support.MessageBuilder.withPayload(2)
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "foo")
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 42)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 43).build();
		output.send(message2);
		output.send(new GenericMessage<>(1));
		output.send(new GenericMessage<>(0));

		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();

		Condition<Message<?>> correlationHeadersForPayload2 = new Condition<Message<?>>() {

			@Override
			public boolean matches(Message<?> value) {
				IntegrationMessageHeaderAccessor accessor = new IntegrationMessageHeaderAccessor(value);
				return "foo".equals(accessor.getCorrelationId()) && 42 == accessor.getSequenceNumber()
						&& 43 == accessor.getSequenceSize();
			}
		};

		if (usesExplicitRouting()) {
			assertThat(receive0.getPayload()).isEqualTo(0);
			assertThat(receive1.getPayload()).isEqualTo(1);
			assertThat(receive2.getPayload()).isEqualTo(2);
			assertThat(receive2).has(correlationHeadersForPayload2);
		}
		else {
			List<Message<?>> receivedMessages = Arrays.asList(receive0, receive1, receive2);
			assertThat(receivedMessages).extracting("payload").containsExactlyInAnyOrder(0, 1, 2);
			Condition<Message<?>> payloadIs2 = new Condition<Message<?>>() {

				@Override
				public boolean matches(Message<?> value) {
					return value.getPayload().equals(2);
				}
			};
			assertThat(receivedMessages).filteredOn(payloadIs2).areExactly(1, correlationHeadersForPayload2);

		}
		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testPartitionedModuleJava() throws Exception {
		Binder binder = getBinder();

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setPartitioned(true);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
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

		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionKeyExtractorClass(PartitionTestSupport.class);
		producerProperties.setPartitionSelectorClass(PartitionTestSupport.class);
		producerProperties.setPartitionCount(3);
		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(producerProperties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partJ.0", output, producerProperties);
		if (usesExplicitRouting()) {
			Object endpoint = extractEndpoint(outputBinding);
			assertThat(getEndpointRouting(endpoint)).
					contains(getExpectedRoutingBaseDestination("partJ.0", "test") + "-' + headers['partition']");
		}

		output.send(new GenericMessage<>(2));
		output.send(new GenericMessage<>(1));
		output.send(new GenericMessage<>(0));

		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();

		if (usesExplicitRouting()) {
			assertThat(receive0.getPayload()).isEqualTo(0);
			assertThat(receive1.getPayload()).isEqualTo(1);
			assertThat(receive2.getPayload()).isEqualTo(2);
		}
		else {
			List<Message<?>> receivedMessages = Arrays.asList(receive0, receive1, receive2);
			assertThat(receivedMessages).extracting("payload").containsExactlyInAnyOrder(0, 1, 2);
		}

		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testAnonymousGroup() throws Exception {
		Binder binder = getBinder();
		BindingProperties producerBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel output = createBindableChannel("output", producerBindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer("defaultGroup.0", output,
				producerBindingProperties.getProducer());

		QueueChannel input1 = new QueueChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<MessageChannel> binding1 = binder.bindConsumer("defaultGroup.0", null, input1,
				consumerProperties);

		QueueChannel input2 = new QueueChannel();
		Binding<MessageChannel> binding2 = binder.bindConsumer("defaultGroup.0", null, input2,
				consumerProperties);
		//Since we don't provide any topic info, let Kafka bind the consumer successfully
		Thread.sleep(1000);
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));

		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload1);

		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload1);

		binding2.unbind();

		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));

		binding2 = binder.bindConsumer("defaultGroup.0", null, input2, consumerProperties);
		//Since we don't provide any topic info, let Kafka bind the consumer successfully
		Thread.sleep(1000);
		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload3.getBytes()));

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
	@SuppressWarnings("unchecked")
	public void testSyncProducerMetadata() throws Exception {
		Binder binder = getBinder(createConfigurationProperties());
		DirectChannel output = new DirectChannel();
		String testTopicName = UUID.randomUUID().toString();
		ExtendedProducerProperties<KafkaProducerProperties> properties = createProducerProperties();
		properties.getExtension().setSync(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output, properties);
		DirectFieldAccessor accessor = new DirectFieldAccessor(extractEndpoint(producerBinding));
		KafkaProducerMessageHandler wrappedInstance = (KafkaProducerMessageHandler) accessor.getWrappedInstance();
		assertThat(new DirectFieldAccessor(wrappedInstance).getPropertyValue("sync").equals(Boolean.TRUE))
				.withFailMessage("Kafka Sync Producer should have been enabled.");
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoCreateTopicsDisabledFailsIfTopicMissing() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		configurationProperties.setAutoCreateTopics(false);
		Binder binder = getBinder(configurationProperties);
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		setMetadataRetryOperations(binder, metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		String testTopicName = "nonexisting" + System.currentTimeMillis();
		try {
			binder.bindConsumer(testTopicName, "test", output, consumerProperties);
			fail();
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(BinderException.class);
			assertThat(e).hasMessageContaining("Topic " + testTopicName + " does not exist");
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoConfigureTopicsDisabledSucceedsIfTopicExisting() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		final ZkUtils zkUtils = getZkUtils(configurationProperties);

		String testTopicName = "existing" + System.currentTimeMillis();
		invokeCreateTopic(zkUtils, testTopicName, 5, 1, new Properties());

		configurationProperties.setAutoCreateTopics(false);
		Binder binder = getBinder(configurationProperties);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<MessageChannel> binding = binder.bindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testPartitionCountIncreasedIfAutoAddPartitionsSet() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		final ZkUtils zkUtils = getZkUtils(configurationProperties);

		String testTopicName = "existing" + System.currentTimeMillis();
		invokeCreateTopic(zkUtils, testTopicName, 6, 1, new Properties());
		configurationProperties.setMinPartitionCount(6);
		configurationProperties.setAutoAddPartitions(true);
		Binder binder = getBinder(configurationProperties);
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		setMetadataRetryOperations(binder, metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<?> binding = binder.bindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
		assertThat(invokePartitionSize(testTopicName, zkUtils)).isEqualTo(6);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoAddPartitionsDisabledFailsIfTopicUnderpartitioned() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		final ZkClient zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
				configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);

		final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);

		String testTopicName = "existing" + System.currentTimeMillis();
		invokeCreateTopic(zkUtils, testTopicName, 1, 1, new Properties());
		configurationProperties.setAutoAddPartitions(false);
		Binder binder = getBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		//		binder.setApplicationContext(context);
		//		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		// this consumer must consume from partition 2
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(2);
		Binding binding = null;
		try {
			binding = binder.bindConsumer(testTopicName, "test", output, consumerProperties);
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(BinderException.class);
			assertThat(e)
					.hasMessageContaining("The number of expected partitions was: 3, but 1 has been found instead");
		}
		finally {
			if (binding != null) {
				binding.unbind();
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoAddPartitionsDisabledSucceedsIfTopicPartitionedCorrectly() throws Exception {
		Binding<?> binding = null;
		try {
			KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

			final ZkClient zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
					configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
					ZKStringSerializer$.MODULE$);

			final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);

			String testTopicName = "existing" + System.currentTimeMillis();
			invokeCreateTopic(zkUtils, testTopicName, 6, 1, new Properties());
			configurationProperties.setAutoAddPartitions(false);
			Binder binder = getBinder(configurationProperties);
			RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
			metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
			FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
			backOffPolicy.setBackOffPeriod(1000);
			metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
			setMetadataRetryOperations(binder, metatadataRetrievalRetryOperations);
			DirectChannel output = new DirectChannel();
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			// this consumer must consume from partition 2
			consumerProperties.setInstanceCount(3);
			consumerProperties.setInstanceIndex(2);
			consumerProperties.getExtension().setAutoRebalanceEnabled(false);

			binding = binder.bindConsumer(testTopicName, "test-x", output, consumerProperties);

			TopicPartitionInitialOffset[] listenedPartitions = TestUtils.getPropertyValue(binding,
					"endpoint.messageListenerContainer.containerProperties.topicPartitions",
					TopicPartitionInitialOffset[].class);
			assertThat(listenedPartitions).hasSize(2);
			assertThat(listenedPartitions).contains(new TopicPartitionInitialOffset(testTopicName, 2),
					new TopicPartitionInitialOffset(testTopicName, 5));
			int partitions = invokePartitionSize(testTopicName, zkUtils);
			assertThat(partitions).isEqualTo(6);
		}
		finally {
			if (binding != null) {
				binding.unbind();
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testPartitionCountNotReduced() throws Exception {
		String testTopicName = "existing" + System.currentTimeMillis();

		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		final ZkClient zkClient;
		zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
				configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);

		final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);
		invokeCreateTopic(zkUtils, testTopicName, 6, 1, new Properties());
		configurationProperties.setAutoAddPartitions(true);
		Binder binder = getBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		setMetadataRetryOperations(binder, metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<?> binding = binder.bindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();

		assertThat(partitionSize(testTopicName)).isEqualTo(6);
	}

	@Override
	protected void binderBindUnbindLatency() throws InterruptedException {
		Thread.sleep(500);
	}

	private final class FailingInvocationCountingMessageHandler implements MessageHandler {

		private int invocationCount;

		private final LinkedHashMap<Long, Message<?>> receivedMessages = new LinkedHashMap<>();

		private final CountDownLatch latch;

		private FailingInvocationCountingMessageHandler(int latchSize) {
			latch = new CountDownLatch(latchSize);
		}

		private FailingInvocationCountingMessageHandler() {
			this(1);
		}

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			invocationCount++;
			Long offset = message.getHeaders().get(KafkaBinderTests.this.getKafkaOffsetHeaderKey(), Long.class);
			// using the offset as key allows to ensure that we don't store duplicate
			// messages on retry
			if (!receivedMessages.containsKey(offset)) {
				receivedMessages.put(offset, message);
				latch.countDown();
			}
			throw new RuntimeException();
		}

		public LinkedHashMap<Long, Message<?>> getReceivedMessages() {
			return receivedMessages;
		}

		public int getInvocationCount() {
			return invocationCount;
		}

		public CountDownLatch getLatch() {
			return latch;
		}
	}
}
