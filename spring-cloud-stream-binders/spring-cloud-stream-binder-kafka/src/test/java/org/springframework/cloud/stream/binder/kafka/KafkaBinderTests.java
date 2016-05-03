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

package org.springframework.cloud.stream.binder.kafka;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsArrayContaining.hasItemInArray;
import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.TestUtils;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.test.junit.kafka.KafkaTestSupport;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.TopicNotFoundException;
import org.springframework.integration.kafka.support.ProducerConfiguration;
import org.springframework.integration.kafka.support.ProducerMetadata;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;


/**
 * Integration tests for the {@link KafkaMessageChannelBinder}.
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class KafkaBinderTests extends PartitionCapableBinderTests<KafkaTestBinder, ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>> {

	private final String CLASS_UNDER_TEST_NAME = KafkaMessageChannelBinder.class.getSimpleName();

	@ClassRule
	public static KafkaTestSupport kafkaTestSupport = new KafkaTestSupport(true);

	private KafkaTestBinder binder;

	@Override
	protected void binderBindUnbindLatency() throws InterruptedException {
		Thread.sleep(500);
	}

	@Override
	protected KafkaTestBinder getBinder() {
		if (binder == null) {
			binder = new KafkaTestBinder(kafkaTestSupport, new KafkaBinderConfigurationProperties());
		}
		return binder;
	}

	@Override
	protected ExtendedConsumerProperties<KafkaConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new KafkaConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<KafkaProducerProperties> createProducerProperties() {
		return new ExtendedProducerProperties<>(new KafkaProducerProperties());
	}

	@Before
	public void init() {
		String multiplier = System.getenv("KAFKA_TIMEOUT_MULTIPLIER");
		if (multiplier != null) {
			timeoutMultiplier = Double.parseDouble(multiplier);
		}
	}

	@Override
	protected boolean usesExplicitRouting() {
		return false;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	public Spy spyOn(final String name) {
		throw new UnsupportedOperationException("'spyOn' is not used by Kafka tests");
	}

	@Test
	public void testDlqAndRetry() {
		KafkaTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		QueueChannel  dlqChannel = new QueueChannel();
		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setEnableDlq(true);
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0", "testGroup",
				moduleInputChannel, consumerProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);

		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(
				"error.retryTest." + uniqueBindingId + ".0.testGroup", null, dlqChannel, dlqConsumerProperties);

		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage = MessageBuilder.withPayload(testMessagePayload).build();
		moduleOutputChannel.send(testMessage);

		Message<?> receivedMessage = receive(dlqChannel, 3);
		assertNotNull(receivedMessage);
		assertEquals(testMessagePayload, receivedMessage.getPayload());
		assertThat(handler.getInvocationCount(), equalTo(consumerProperties.getMaxAttempts()));
		dlqConsumerBinding.unbind();
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidateKafkaTopicName() {
		KafkaMessageChannelBinder.validateTopicName("foo:bar");
	}

	@Test
	public void testCompression() throws Exception {
		final ProducerMetadata.CompressionType[] codecs = new ProducerMetadata.CompressionType[] {
				ProducerMetadata.CompressionType.none,
				ProducerMetadata.CompressionType.gzip,
				ProducerMetadata.CompressionType.snappy };

		byte[] ratherBigPayload = new byte[2048];
		Arrays.fill(ratherBigPayload, (byte) 65);
		KafkaTestBinder binder = getBinder();

		for (ProducerMetadata.CompressionType codec : codecs) {
			DirectChannel moduleOutputChannel = new DirectChannel();
			QueueChannel moduleInputChannel = new QueueChannel();
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
			producerProperties.getExtension().setCompressionType(codec);
			Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel, producerProperties);
			Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "test", moduleInputChannel, createConsumerProperties());
			Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
			// Let the consumer actually bind to the producer before sending a msg
			binderBindUnbindLatency();
			moduleOutputChannel.send(message);
			Message<?> inbound = receive(moduleInputChannel);
			assertNotNull(inbound);
			assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
			producerBinding.unbind();
			consumerBinding.unbind();
		}
	}

	@Test
	public void testCustomPartitionCountOverridesDefaultIfLarger() throws Exception {

		byte[] ratherBigPayload = new byte[2048];
		Arrays.fill(ratherBigPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = new KafkaBinderConfigurationProperties();
		binderConfiguration.setMinPartitionCount(10);
		KafkaTestBinder binder = new KafkaTestBinder(kafkaTestSupport, binderConfiguration);

		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(10));
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testCustomPartitionCountDoesNotOverridePartitioningIfSmaller() throws Exception {

		byte[] ratherBigPayload = new byte[2048];
		Arrays.fill(ratherBigPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = new KafkaBinderConfigurationProperties();
		binderConfiguration.setMinPartitionCount(5);
		KafkaTestBinder binder = new KafkaTestBinder(kafkaTestSupport, binderConfiguration);

		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(5);
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(5));
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testCustomPartitionCountOverridesPartitioningIfLarger() throws Exception {

		byte[] ratherBigPayload = new byte[2048];
		Arrays.fill(ratherBigPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = new KafkaBinderConfigurationProperties();
		binderConfiguration.setMinPartitionCount(5);
		KafkaTestBinder binder = new KafkaTestBinder(kafkaTestSupport, binderConfiguration);

		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(5);
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(5));
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDefaultConsumerStartsAtEarliest() throws Exception {
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(new ZookeeperConnect(kafkaTestSupport.getZkConnectString()),
				kafkaTestSupport.getBrokerAddress(), kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		binder.bindProducer(testTopicName, output, createProducerProperties());
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		binder.bindConsumer(testTopicName, "startOffsets", input1, createConsumerProperties());
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1, not(nullValue()));
		assertThat(new String(receivedMessage1.getPayload()), equalTo(testPayload1));
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2, not(nullValue()));
		assertThat(new String(receivedMessage2.getPayload()), equalTo(testPayload2));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEarliest() throws Exception {
		KafkaTestBinder binder = getBinder();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		binder.bindProducer(testTopicName, output, createProducerProperties());
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		ExtendedConsumerProperties<KafkaConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setStartOffset(KafkaMessageChannelBinder.StartOffset.earliest);
		binder.bindConsumer(testTopicName, "startOffsets", input1, properties);
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1, not(nullValue()));
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2, not(nullValue()));
		assertThat(new String(receivedMessage2.getPayload()), equalTo(testPayload2));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReset() throws Exception {
		KafkaTestBinder binder = getBinder();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();

		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output, createProducerProperties());
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		ExtendedConsumerProperties<KafkaConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setResetOffsets(true);
		properties.getExtension().setStartOffset(KafkaMessageChannelBinder.StartOffset.earliest);
		Binding<MessageChannel> consumerBinding =
				binder.bindConsumer(testTopicName, "startOffsets", input1, properties);
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1, not(nullValue()));
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2, not(nullValue()));
		assertThat(new String(receivedMessage2.getPayload()), equalTo(testPayload2));
		consumerBinding.unbind();

		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload3.getBytes()));

		ExtendedConsumerProperties<KafkaConsumerProperties> properties2 = createConsumerProperties();
		properties2.getExtension().setResetOffsets(true);
		properties2.getExtension().setStartOffset(KafkaMessageChannelBinder.StartOffset.earliest);
		consumerBinding =
				binder.bindConsumer(testTopicName, "startOffsets", input1, properties2);
		Message<byte[]> receivedMessage4 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage4, not(nullValue()));
		assertThat(new String(receivedMessage4.getPayload()), equalTo(testPayload1));
		Message<byte[]> receivedMessage5 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage5, not(nullValue()));
		assertThat(new String(receivedMessage5.getPayload()), equalTo(testPayload2));
		Message<byte[]> receivedMessage6 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage6, not(nullValue()));
		assertThat(new String(receivedMessage6.getPayload()), equalTo(testPayload3));
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testResume() throws Exception {
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(new ZookeeperConnect(kafkaTestSupport.getZkConnectString()),
				kafkaTestSupport.getBrokerAddress(), kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output, createProducerProperties());
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		ExtendedConsumerProperties<KafkaConsumerProperties> firstConsumerProperties = createConsumerProperties();
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1, firstConsumerProperties);
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1, not(nullValue()));
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2, not(nullValue()));
		assertThat(new String(receivedMessage2.getPayload()), equalTo(testPayload2));
		consumerBinding.unbind();

		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload3.getBytes()));

		consumerBinding =
				binder.bindConsumer(testTopicName, "startOffsets", input1, createConsumerProperties());
		Message<byte[]> receivedMessage3 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage3, not(nullValue()));
		assertThat(new String(receivedMessage3.getPayload()), equalTo(testPayload3));
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	public void testSyncProducerMetadata() throws Exception {
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(new ZookeeperConnect(kafkaTestSupport.getZkConnectString()),
				kafkaTestSupport.getBrokerAddress(), kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();

		DirectChannel output = new DirectChannel();

		String testTopicName = UUID.randomUUID().toString();
		ExtendedProducerProperties<KafkaProducerProperties> properties = createProducerProperties();
		properties.getExtension().setSync(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output, properties);
		DirectFieldAccessor accessor = new DirectFieldAccessor(extractEndpoint(producerBinding));
		MessageHandler handler = (MessageHandler) accessor.getPropertyValue("handler");
		DirectFieldAccessor accessor1 = new DirectFieldAccessor(handler);
		ProducerConfiguration producerConfiguration = (ProducerConfiguration) accessor1.getPropertyValue("producerConfiguration");
		assertTrue("Kafka Sync Producer should have been enabled.", producerConfiguration.getProducerMetadata().isSync());
		producerBinding.unbind();
	}

	@Test
	public void testAutoConfigureTopicsDisabledFailsIfTopicMissing() throws Exception {

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				new ZookeeperConnect(kafkaTestSupport.getZkConnectString()), kafkaTestSupport.getBrokerAddress(),
				kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		binder.setAutoConfigureTopics(false);
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		String testTopicName = "nonexisting"  + System.currentTimeMillis();
		try {
			binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
			fail();
		}
		catch (Exception e) {
			assertTrue(e instanceof BinderException);
			assertThat(e.getCause(), instanceOf(TopicNotFoundException.class));
			assertThat(e.getCause().getMessage(), containsString(testTopicName));
		}

		try {
			binder.getConnectionFactory().getPartitions(testTopicName);
			fail();
		}
		catch (Exception e) {
			assertThat(e, instanceOf(TopicNotFoundException.class));
		}
	}

	@Test
	public void testAutoConfigureTopicsDisabledSucceedsIfTopicExisting() throws Exception {

		String testTopicName = "existing"  + System.currentTimeMillis();
		AdminUtils.createTopic(kafkaTestSupport.getZkClient(), testTopicName, 5, 1, new Properties());

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				new ZookeeperConnect(kafkaTestSupport.getZkConnectString()), kafkaTestSupport.getBrokerAddress(),
				kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		binder.setAutoConfigureTopics(false);
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<MessageChannel> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
	}

	@Test
	public void testAutoConfigureTopicsDisabledFailsIfTopicUnderpartitioned() throws Exception {

		String testTopicName = "existing"  + System.currentTimeMillis();
		AdminUtils.createTopic(kafkaTestSupport.getZkClient(), testTopicName, 1, 1, new Properties());

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				new ZookeeperConnect(kafkaTestSupport.getZkConnectString()), kafkaTestSupport.getBrokerAddress(),
				kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		binder.setAutoConfigureTopics(false);

		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		// this consumer must consume from partition 2
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(2);
		try {
			binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		}
		catch (Exception e) {
			assertThat(e, instanceOf(BinderException.class));
			assertThat(e.getCause(), instanceOf(IllegalStateException.class));
			assertThat(e.getCause().getMessage(),
					containsString("The number of expected partitions was: 3, but 1 has been found instead"));
		}
	}

	@Test
	public void testAutoConfigureTopicsDisabledSucceedsIfTopicPartitionedCorrectly() throws Exception {

		String testTopicName = "existing"  + System.currentTimeMillis();
		AdminUtils.createTopic(kafkaTestSupport.getZkClient(), testTopicName, 6, 1, new Properties());

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				new ZookeeperConnect(kafkaTestSupport.getZkConnectString()), kafkaTestSupport.getBrokerAddress(),
				kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		binder.setAutoConfigureTopics(false);
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		// this consumer must consume from partition 2
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(2);

		Binding<?> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);

		Partition[] listenedPartitions
				= TestUtils.getPropertyValue(binding, "endpoint.val$messageListenerContainer.partitions", Partition[].class);

		assertThat(listenedPartitions, arrayWithSize(2));
		assertThat(listenedPartitions, hasItemInArray(new Partition(testTopicName, 2)));
		assertThat(listenedPartitions, hasItemInArray(new Partition(testTopicName, 5)));
		Collection<Partition> partitions = binder.getConnectionFactory().getPartitions(testTopicName);
		assertThat(partitions, IsCollectionWithSize.hasSize(6));
		binding.unbind();
	}

	@Test
	public void testAutoConfigureTopicsEnabledSucceeds() throws Exception {
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				new ZookeeperConnect(kafkaTestSupport.getZkConnectString()), kafkaTestSupport.getBrokerAddress(),
				kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		binder.setAutoConfigureTopics(true);
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		String testTopicName = "nonexisting"  + System.currentTimeMillis();
		Binding<?> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
	}

	private static class FailingInvocationCountingMessageHandler implements MessageHandler {

		private int invocationCount = 0;

		public FailingInvocationCountingMessageHandler() {
		}

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			invocationCount++;
			throw new RuntimeException();
		}

		public int getInvocationCount() {
			return invocationCount;
		}
	}

	@Test
	public void testPartitionCountNotReduced() throws Exception {
		String testTopicName = "existing"  + System.currentTimeMillis();
		AdminUtils.createTopic(kafkaTestSupport.getZkClient(), testTopicName, 6, 1, new Properties());

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				new ZookeeperConnect(kafkaTestSupport.getZkConnectString()), kafkaTestSupport.getBrokerAddress(),
				kafkaTestSupport.getZkConnectString());

		GenericApplicationContext context = new GenericApplicationContext();
		binder.setAutoConfigureTopics(true);
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<?> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
		TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(testTopicName, kafkaTestSupport.getZkClient());
		assertThat(topicMetadata.partitionsMetadata().size(), equalTo(6));
	}

	@Test
	public void testPartitionCountIncreasedIfAutoConfigureTopicsSet() throws Exception {
		String testTopicName = "existing"  + System.currentTimeMillis();
		AdminUtils.createTopic(kafkaTestSupport.getZkClient(), testTopicName, 1, 1, new Properties());

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				new ZookeeperConnect(kafkaTestSupport.getZkConnectString()), kafkaTestSupport.getBrokerAddress(),
				kafkaTestSupport.getZkConnectString());

		binder.setDefaultMinPartitionCount(6);

		GenericApplicationContext context = new GenericApplicationContext();
		binder.setAutoConfigureTopics(true);
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<?> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
		TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(testTopicName, kafkaTestSupport.getZkClient());
		assertThat(topicMetadata.partitionsMetadata().size(), equalTo(6));
	}
}
