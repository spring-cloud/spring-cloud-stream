/*
 * Copyright 2017-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 1.2.2
 *
 */
class KafkaBinderUnitTests {

	@Test
	void propertyOverrides() throws Exception {
		KafkaProperties kafkaProperties = new TestKafkaProperties();
		KafkaBinderConfigurationProperties binderConfigurationProperties = new KafkaBinderConfigurationProperties(
				kafkaProperties);
		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
				binderConfigurationProperties, kafkaProperties, prop -> {
		});
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				binderConfigurationProperties, provisioningProvider);
		KafkaConsumerProperties consumerProps = new KafkaConsumerProperties();
		ExtendedConsumerProperties<KafkaConsumerProperties> ecp = new ExtendedConsumerProperties<KafkaConsumerProperties>(
				consumerProps);
		Method method = KafkaMessageChannelBinder.class.getDeclaredMethod(
				"createKafkaConsumerFactory", boolean.class, String.class,
				ExtendedConsumerProperties.class, String.class, String.class);
		method.setAccessible(true);

		// test default for anon
		Object factory = method.invoke(binder, true, "foo-1", ecp, "foo.consumer", "foo");
		Map<?, ?> configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
				.isEqualTo("latest");

		// test default for named
		factory = method.invoke(binder, false, "foo-2", ecp, "foo.consumer", "foo");
		configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
				.isEqualTo("earliest");

		// binder level setting
		binderConfigurationProperties.setConfiguration(Collections
				.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"));
		factory = method.invoke(binder, false, "foo-3", ecp, "foo.consumer", "foo");
		configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
				.isEqualTo("latest");

		// consumer level setting
		consumerProps.setConfiguration(Collections
				.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		factory = method.invoke(binder, false, "foo-4", ecp, "foo.consumer", "foo");
		configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
				.isEqualTo("earliest");
	}

	@Test
	void mergedConsumerProperties() {
		KafkaProperties bootProps = new TestKafkaProperties();
		bootProps.getConsumer().getProperties()
				.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "bar");
		KafkaBinderConfigurationProperties props = new KafkaBinderConfigurationProperties(
				bootProps);
		assertThat(props.mergedConsumerConfiguration()
				.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("bar");
		props.getConfiguration().put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "baz");
		assertThat(props.mergedConsumerConfiguration()
				.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("baz");
		props.getConsumerProperties().put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "qux");
		assertThat(props.mergedConsumerConfiguration()
				.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("qux");
	}

	@Test
	void mergedProducerProperties() {
		KafkaProperties bootProps = new TestKafkaProperties();
		bootProps.getProducer().getProperties().put(ProducerConfig.RETRIES_CONFIG, "bar");
		KafkaBinderConfigurationProperties props = new KafkaBinderConfigurationProperties(
				bootProps);
		assertThat(props.mergedProducerConfiguration().get(ProducerConfig.RETRIES_CONFIG))
				.isEqualTo("bar");
		props.getConfiguration().put(ProducerConfig.RETRIES_CONFIG, "baz");
		assertThat(props.mergedProducerConfiguration().get(ProducerConfig.RETRIES_CONFIG))
				.isEqualTo("baz");
		props.getProducerProperties().put(ProducerConfig.RETRIES_CONFIG, "qux");
		assertThat(props.mergedProducerConfiguration().get(ProducerConfig.RETRIES_CONFIG))
				.isEqualTo("qux");
	}

	@Test
	void offsetResetWithGroupManagementEarliest() throws Exception {
		testOffsetResetWithGroupManagement(true, true, "foo-100",
				"testOffsetResetWithGroupManagementEarliest");
	}

	@Test
	void offsetResetWithGroupManagementLatest() throws Throwable {
		testOffsetResetWithGroupManagement(false, true, "foo-101",
				"testOffsetResetWithGroupManagementLatest");
	}

	@Test
	void offsetResetWithManualAssignmentEarliest() throws Exception {
		testOffsetResetWithGroupManagement(true, false, "foo-102",
				"testOffsetResetWithManualAssignmentEarliest");
	}

	@Test
	void offsetResetWithGroupManualAssignmentLatest() throws Throwable {
		testOffsetResetWithGroupManagement(false, false, "foo-103",
				"testOffsetResetWithGroupManualAssignmentLatest");
	}

	void testOffsetResetWithGroupManagement(final boolean earliest,
			boolean groupManage, String topic, String group) throws Exception {

		final List<TopicPartition> partitions = new ArrayList<>();
		partitions.add(new TopicPartition(topic, 0));
		partitions.add(new TopicPartition(topic, 1));
		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
				new TestKafkaProperties());
		KafkaTopicProvisioner provisioningProvider = mock(KafkaTopicProvisioner.class);
		ConsumerDestination dest = mock(ConsumerDestination.class);
		given(dest.getName()).willReturn(topic);
		given(provisioningProvider.provisionConsumerDestination(anyString(), anyString(),
				any())).willReturn(dest);
		final AtomicInteger part = new AtomicInteger();
		willAnswer(i -> {
			return partitions.stream().map(p -> new PartitionInfo(topic,
					part.getAndIncrement(), null, null, null))
					.collect(Collectors.toList());
		}).given(provisioningProvider).getPartitionsForTopic(anyInt(), anyBoolean(),
				any(), any());
		@SuppressWarnings("unchecked")
		final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
		final CountDownLatch latch = new CountDownLatch(1);
		willAnswer(i -> {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return new ConsumerRecords<>(Collections.emptyMap());
		}).given(consumer).poll(any(Duration.class));
		willAnswer(i -> {
			((org.apache.kafka.clients.consumer.ConsumerRebalanceListener) i
					.getArgument(1)).onPartitionsAssigned(partitions);
			latch.countDown();
			return null;
		}).given(consumer).subscribe(eq(Collections.singletonList(topic)), any());
		willAnswer(i -> {
			latch.countDown();
			return null;
		}).given(consumer).seekToBeginning(any());
		willAnswer(i -> {
			latch.countDown();
			return null;
		}).given(consumer).seekToEnd(any());
		class Customizer implements ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> {

			@Override
			public void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName,
					String group) {

				container.getContainerProperties().setMissingTopicsFatal(false);
			}

		}
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				configurationProperties, provisioningProvider, new Customizer(), null) {

			@Override
			protected ConsumerFactory<?, ?> createKafkaConsumerFactory(boolean anonymous,
					String consumerGroup,
					ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties, String beanName, String destination) {

				return new ConsumerFactory<byte[], byte[]>() {

					@Override
					public Consumer<byte[], byte[]> createConsumer() {
						return consumer;
					}

					@Override
					public Consumer<byte[], byte[]> createConsumer(String arg0) {
						return consumer;
					}

					@Override
					public Consumer<byte[], byte[]> createConsumer(String arg0,
							String arg1) {
						return consumer;
					}

					@Override
					public Consumer<byte[], byte[]> createConsumer(String groupId,
							String clientIdPrefix, String clientIdSuffix) {
						return consumer;
					}

					@Override
					public boolean isAutoCommit() {
						return false;
					}

					@Override
					public Map<String, Object> getConfigurationProperties() {
						Map<String, Object> props = new HashMap<>();
						props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
								earliest ? "earliest" : "latest");
						props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
						return props;
					}

					@Override
					public Deserializer<byte[]> getKeyDeserializer() {
						return null;
					}

					@Override
					public Deserializer<byte[]> getValueDeserializer() {
						return null;
					}

				};
			}

		};
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		MessageChannel channel = new DirectChannel();
		KafkaConsumerProperties extension = new KafkaConsumerProperties();
		extension.setResetOffsets(true);
		extension.setAutoRebalanceEnabled(groupManage);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(
				extension);
		consumerProperties.setInstanceCount(1);
		Binding<MessageChannel> messageChannelBinding = binder.bindConsumer(topic, group,
				channel, consumerProperties);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		if (!groupManage) {
			@SuppressWarnings("unchecked")
			ArgumentCaptor<Set<TopicPartition>> captor = ArgumentCaptor.forClass(Set.class);
			if (earliest) {
				verify(consumer).seekToBeginning(captor.capture());
			}
			else {
				verify(consumer).seekToEnd(captor.capture());
			}
			assertThat(captor.getValue()).containsExactlyInAnyOrderElementsOf(partitions);
		}
		else {
			@SuppressWarnings("unchecked")
			ArgumentCaptor<List<TopicPartition>> captor = ArgumentCaptor.forClass(List.class);
			if (earliest) {
				verify(consumer).seekToBeginning(captor.capture());
			}
			else {
				verify(consumer).seekToEnd(captor.capture());
			}
			assertThat(captor.getValue()).containsExactlyInAnyOrderElementsOf(partitions);
		}
		messageChannelBinding.unbind();
	}

}
