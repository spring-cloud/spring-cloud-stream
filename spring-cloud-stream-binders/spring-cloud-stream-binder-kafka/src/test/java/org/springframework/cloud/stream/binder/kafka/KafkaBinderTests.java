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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderPropertyKeys;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.test.junit.kafka.KafkaTestSupport;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.integration.kafka.listener.MessageListener;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import kafka.api.OffsetRequest;


/**
 * Integration tests for the {@link KafkaMessageChannelBinder}.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class KafkaBinderTests extends PartitionCapableBinderTests {

	private final String CLASS_UNDER_TEST_NAME = KafkaMessageChannelBinder.class.getSimpleName();

	static {
		System.setProperty("SCS_KAFKA_TEST_EMBEDDED", "true");
	}

	@ClassRule
	public static KafkaTestSupport kafkaTestSupport = new KafkaTestSupport();

	private KafkaTestBinder binder;

	@Override
	protected void binderBindUnbindLatency() throws InterruptedException {
		Thread.sleep(500);
	}

	@Override
	protected Binder<MessageChannel> getBinder() {
		if (binder == null) {
			binder = createKafkaTestBinder();
		}
		return binder;
	}

	@Before
	public void init() {
		String multiplier = System.getenv("KAFKA_TIMEOUT_MULTIPLIER");
		if (multiplier != null) {
			timeoutMultiplier = Double.parseDouble(multiplier);
		}
	}

	protected KafkaTestBinder createKafkaTestBinder() {
		return new KafkaTestBinder(kafkaTestSupport, KafkaMessageChannelBinder.Mode.embeddedHeaders);
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
		KafkaMessageChannelBinder.validateTopicName(name);

		KafkaTestBinder binderWrapper = (KafkaTestBinder) getBinder();
		// Rewind offset, as tests will have typically already sent the messages we're trying to consume

		KafkaMessageListenerContainer messageListenerContainer = binderWrapper.getCoreBinder().createMessageListenerContainer(
				new Properties(), UUID.randomUUID().toString(), 1, name, OffsetRequest.EarliestTime());

		final BlockingQueue<KafkaMessage> messages = new ArrayBlockingQueue<KafkaMessage>(10);

		messageListenerContainer.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(KafkaMessage message) {
				messages.offer(message);
			}
		});


		return new Spy() {

			@Override
			public Object receive(boolean expectNull) throws Exception {
				return messages.poll(expectNull ? 50 : 5000, TimeUnit.MILLISECONDS);
			}
		};

	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidateKafkaTopicName() {
		KafkaMessageChannelBinder.validateTopicName("foo:bar");
	}

	@Test
	public void testCompression() throws Exception {
		final String[] codecs = new String[] { null, "none", "gzip", "snappy" };

		byte[] ratherBigPayload = new byte[2048];
		Arrays.fill(ratherBigPayload, (byte) 65);
		Binder<MessageChannel> binder = getBinder();

		for (String codec : codecs) {
			DirectChannel moduleOutputChannel = new DirectChannel();
			QueueChannel moduleInputChannel = new QueueChannel();
			Properties props = new Properties();
			if (codec != null) {
				props.put(KafkaMessageChannelBinder.COMPRESSION_CODEC, codec);
			}
			Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel, props);
			Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "test", moduleInputChannel, null);
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
		KafkaTestBinder binder = (KafkaTestBinder) getBinder();

		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		Properties producerProperties = new Properties();
		producerProperties.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "10");
		Properties consumerProperties = new Properties();
		consumerProperties.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "10");
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
	public void testCustomPartitionCountDoesNotOverrideModuleCountAndConcurrencyIfSmaller() throws Exception {

		byte[] ratherBigPayload = new byte[2048];
		Arrays.fill(ratherBigPayload, (byte) 65);
		KafkaTestBinder binder = (KafkaTestBinder) getBinder();


		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		Properties producerProps = new Properties();
		producerProps.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "5");
		producerProps.put(BinderPropertyKeys.NEXT_MODULE_CONCURRENCY, "6");
		Properties consumerProps = new Properties();
		consumerProps.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "5");
		consumerProps.put(BinderPropertyKeys.CONCURRENCY, "6");
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProps);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProps);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(6));
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testCustomPartitionCountOverridesModuleCountAndConcurrencyIfLarger() throws Exception {

		byte[] ratherBigPayload = new byte[2048];
		Arrays.fill(ratherBigPayload, (byte) 65);
		KafkaTestBinder binder = (KafkaTestBinder) getBinder();

		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		Properties producerProps = new Properties();
		producerProps.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "6");
		producerProps.put(BinderPropertyKeys.NEXT_MODULE_CONCURRENCY, "5");
		Properties consumerProps = new Properties();
		consumerProps.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "6");
		consumerProps.put(BinderPropertyKeys.CONCURRENCY, "5");
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProps);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProps);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(6));
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testCustomPartitionCountDoesNotOverridePartitioningIfSmaller() throws Exception {

		byte[] ratherBigPayload = new byte[2048];
		Arrays.fill(ratherBigPayload, (byte) 65);
		KafkaTestBinder binder = (KafkaTestBinder) getBinder();

		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		Properties producerProperties = new Properties();
		producerProperties.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "3");
		producerProperties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "5");
		producerProperties.put(BinderPropertyKeys.PARTITION_KEY_EXPRESSION, "payload");
		Properties consumerProperties = new Properties();
		consumerProperties.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "3");
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
		KafkaTestBinder binder = (KafkaTestBinder) getBinder();

		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		Properties producerProperties = new Properties();
		producerProperties.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "5");
		producerProperties.put(BinderPropertyKeys.NEXT_MODULE_COUNT, "3");
		producerProperties.put(BinderPropertyKeys.PARTITION_KEY_EXPRESSION, "payload");
		Properties consumerProperties = new Properties();
		consumerProperties.put(BinderPropertyKeys.MIN_PARTITION_COUNT, "5");
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
	public void testDefaultConsumerStartsAtLatest() throws Exception {
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(new ZookeeperConnect(kafkaTestSupport.getZkConnectString()),
				kafkaTestSupport.getBrokerAddress(), kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		Properties properties = new Properties();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		binder.bindProducer(testTopicName,output,properties);
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		binder.bindConsumer(testTopicName, "startOffsets", input1, properties);
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1, is(nullValue()));
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2, not(nullValue()));
		assertThat(new String(receivedMessage2.getPayload()), equalTo(testPayload2));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEarliest() throws Exception {
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(new ZookeeperConnect(kafkaTestSupport.getZkConnectString()),
				kafkaTestSupport.getBrokerAddress(), kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		binder.setStartOffset(KafkaMessageChannelBinder.StartOffset.earliest);
		DirectChannel output = new DirectChannel();
		Properties properties = new Properties();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		binder.bindProducer(testTopicName,output,properties);
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
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
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(new ZookeeperConnect(kafkaTestSupport.getZkConnectString()),
				kafkaTestSupport.getBrokerAddress(), kafkaTestSupport.getZkConnectString());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.setStartOffset(KafkaMessageChannelBinder.StartOffset.earliest);
		binder.setResetOffsets(true);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		Properties properties = new Properties();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();

		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output, properties);
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
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

		consumerBinding =
				binder.bindConsumer(testTopicName, "startOffsets", input1, properties);
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
		binder.setStartOffset(KafkaMessageChannelBinder.StartOffset.earliest);
		DirectChannel output = new DirectChannel();
		Properties properties = new Properties();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output, properties);
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1, properties);
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
				binder.bindConsumer(testTopicName, "startOffsets", input1, properties);
		Message<byte[]> receivedMessage3 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage3, not(nullValue()));
		assertThat(new String(receivedMessage3.getPayload()), equalTo(testPayload3));
		consumerBinding.unbind();
		producerBinding.unbind();
	}
}
