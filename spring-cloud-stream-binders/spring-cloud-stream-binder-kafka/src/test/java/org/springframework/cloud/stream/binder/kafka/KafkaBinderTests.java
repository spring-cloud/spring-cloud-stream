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

import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderPropertyKeys;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.test.junit.kafka.KafkaTestSupport;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.integration.kafka.listener.MessageListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import kafka.api.OffsetRequest;


/**
 * Integration tests for the {@link KafkaMessageChannelBinder}.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Mark Fisher
 */
public class KafkaBinderTests extends PartitionCapableBinderTests {

	private final String CLASS_UNDER_TEST_NAME = KafkaMessageChannelBinder.class.getSimpleName();

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
		String topic = KafkaMessageChannelBinder.escapeTopicName(name);

		KafkaTestBinder binderWrapper = (KafkaTestBinder) getBinder();
		// Rewind offset, as tests will have typically already sent the messages we're trying to consume

		KafkaMessageListenerContainer messageListenerContainer = binderWrapper.getCoreBinder().createMessageListenerContainer(
				new Properties(), UUID.randomUUID().toString(), 1, topic, OffsetRequest.EarliestTime());

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
			binder.bindProducer("foo.0", moduleOutputChannel, props);
			binder.bindConsumer("foo.0", "test", moduleInputChannel, null);
			Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
			// Let the consumer actually bind to the producer before sending a msg
			binderBindUnbindLatency();
			moduleOutputChannel.send(message);
			Message<?> inbound = moduleInputChannel.receive(2000);
			assertNotNull(inbound);
			assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
			binder.unbindProducers("foo.0");
			binder.unbindConsumers("foo.0", "test");
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
		binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProperties);
		binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(2000);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(10));
		binder.unbindProducers("foo" + uniqueBindingId + ".0");
		binder.unbindConsumers("foo" + uniqueBindingId + ".0", "default");
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
		binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProps);
		binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProps);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(2000);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(6));
		binder.unbindProducers("foo" + uniqueBindingId + ".0");
		binder.unbindConsumers("foo" + uniqueBindingId + ".0", "default");
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
		binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProps);
		binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProps);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(2000);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(6));
		binder.unbindProducers("foo" + uniqueBindingId + ".0");
		binder.unbindConsumers("foo" + uniqueBindingId + ".0", "default");
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
		binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProperties);
		binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(2000);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(5));
		binder.unbindProducers("foo" + uniqueBindingId + ".0");
		binder.unbindConsumers("foo" + uniqueBindingId + ".0", "default");
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
		binder.bindProducer("foo" + uniqueBindingId + ".0", moduleOutputChannel, producerProperties);
		binder.bindConsumer("foo" + uniqueBindingId + ".0", null, moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(ratherBigPayload).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(2000);
		assertNotNull(inbound);
		assertArrayEquals(ratherBigPayload, (byte[]) inbound.getPayload());
		Collection<Partition> partitions = binder.getCoreBinder().getConnectionFactory().getPartitions(
				"foo" + uniqueBindingId + ".0");
		assertThat(partitions, hasSize(5));
		binder.unbindProducers("foo" + uniqueBindingId + ".0");
		binder.unbindConsumers("foo" + uniqueBindingId + ".0", "default");
	}

}
