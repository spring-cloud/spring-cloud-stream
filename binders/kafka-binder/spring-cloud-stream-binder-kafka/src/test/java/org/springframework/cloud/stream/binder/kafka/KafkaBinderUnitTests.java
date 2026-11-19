/*
 * Copyright 2017-present the original author or authors.
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.converter.StringMessageConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Omer Celik
 * @author Soby Chacko
 * @since 1.2.2
 *
 */
class KafkaBinderUnitTests {

	@Test
	void propertyOverrides() throws Exception {
		KafkaProperties kafkaProperties = new TestKafkaProperties();
		KafkaBinderConfigurationProperties binderConfigurationProperties = new KafkaBinderConfigurationProperties(
				kafkaProperties, mock(ObjectProvider.class));
		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
				binderConfigurationProperties, kafkaProperties, prop -> {
		});
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				binderConfigurationProperties, provisioningProvider);
		KafkaConsumerProperties consumerProps = new KafkaConsumerProperties();
		ExtendedConsumerProperties<KafkaConsumerProperties> ecp = new ExtendedConsumerProperties<>(
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
				bootProps, mock(ObjectProvider.class));
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
				bootProps, mock(ObjectProvider.class));
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
				new TestKafkaProperties(), mock(ObjectProvider.class));
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
		}).given(provisioningProvider).getListenedPartitions(anyString(), any(), any(),
			anyInt(), anyBoolean(), anyBoolean(), anyString(), any());
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
					public Consumer<byte[], byte[]> createConsumer(String groupId, String clientIdPrefix,
							String clientIdSuffix, Properties properties) {
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

	@Test
	void dlqProducerFactoryDestroyedOnConsumerUnbind() throws Exception {
		AtomicReference<DefaultKafkaProducerFactory<byte[], byte[]>> capturedFactory = new AtomicReference<>();
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		KafkaMessageChannelBinder binder = createDlqUnitTestBinder("dlq-unbind-topic", "dlq-unbind-group",
				capturedFactory, context);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = dlqConsumerProperties();
		Binding<MessageChannel> binding = binder.bindConsumer("dlq-unbind-topic", "dlq-unbind-group",
				new DirectChannel(), consumerProperties);

		DefaultKafkaProducerFactory<byte[], byte[]> dlqProducerFactory = capturedFactory.get();
		assertThat(dlqProducerFactory).isNotNull();
		verify(dlqProducerFactory, never()).destroy();

		binding.unbind();
		verify(dlqProducerFactory, times(1)).destroy();

		// Closing the binder afterwards must not destroy the same factory twice
		binder.destroy();
		verify(dlqProducerFactory, times(1)).destroy();
		context.close();
	}

	@Test
	void dlqProducerFactoryDestroyedOnBinderDestroyWithoutUnbind() throws Exception {
		AtomicReference<DefaultKafkaProducerFactory<byte[], byte[]>> capturedFactory = new AtomicReference<>();
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		KafkaMessageChannelBinder binder = createDlqUnitTestBinder("dlq-destroy-topic", "dlq-destroy-group",
				capturedFactory, context);

		ConsumerDestination destination = mock(ConsumerDestination.class);
		given(destination.getName()).willReturn("dlq-destroy-topic");
		MessageHandler errorHandler = binder.getErrorMessageHandler(destination, "dlq-destroy-group",
				dlqConsumerProperties());
		assertThat(errorHandler).isNotNull();

		DefaultKafkaProducerFactory<byte[], byte[]> dlqProducerFactory = capturedFactory.get();
		assertThat(dlqProducerFactory).isNotNull();
		verify(dlqProducerFactory, never()).destroy();

		binder.destroy();
		verify(dlqProducerFactory, times(1)).destroy();
		context.close();
	}

	@Test
	void dlqProducerFactoryDestroyedOnUnbindWhenErrorHandlerNotRegisteredAsBean() throws Exception {
		AtomicReference<DefaultKafkaProducerFactory<byte[], byte[]>> capturedFactory = new AtomicReference<>();
		GenericApplicationContext context = new GenericApplicationContext();
		String preRegisteredErrorHandlerBean = "dlq-user-handler-topic.dlq-user-handler-group.errors.handler";
		context.registerBean(preRegisteredErrorHandlerBean, MessageHandler.class, () -> message -> {
		});
		context.refresh();
		KafkaMessageChannelBinder binder = createDlqUnitTestBinder("dlq-user-handler-topic", "dlq-user-handler-group",
				capturedFactory, context);

		Binding<MessageChannel> binding = binder.bindConsumer("dlq-user-handler-topic", "dlq-user-handler-group",
				new DirectChannel(), dlqConsumerProperties());

		DefaultKafkaProducerFactory<byte[], byte[]> dlqProducerFactory = capturedFactory.get();
		assertThat(dlqProducerFactory).isNotNull();
		verify(dlqProducerFactory, never()).destroy();

		binding.unbind();
		verify(dlqProducerFactory, times(1)).destroy();
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void dlqProducerFactoryNotDestroyedWhenProvidedByTransactionManager() throws Exception {
		AtomicReference<DefaultKafkaProducerFactory<byte[], byte[]>> capturedFactory = new AtomicReference<>();
		GenericApplicationContext context = new GenericApplicationContext();
		DefaultKafkaProducerFactory<byte[], byte[]> txProducerFactory = spy(
				new DefaultKafkaProducerFactory<>(Collections.emptyMap()));
		KafkaAwareTransactionManager<byte[], byte[]> txManager = mock(KafkaAwareTransactionManager.class);
		given(txManager.getProducerFactory()).willReturn(txProducerFactory);
		context.registerBean("customTxManager", KafkaAwareTransactionManager.class, () -> txManager);
		context.refresh();

		KafkaMessageChannelBinder binder = createDlqUnitTestBinder("dlq-tx-topic", "dlq-tx-group",
				capturedFactory, context);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = dlqConsumerProperties();
		consumerProperties.getExtension().setTransactionManager("customTxManager");
		Binding<MessageChannel> binding = binder.bindConsumer("dlq-tx-topic", "dlq-tx-group",
				new DirectChannel(), consumerProperties);

		assertThat(capturedFactory.get()).isNull();
		binding.unbind();
		binder.destroy();
		verify(txProducerFactory, never()).destroy();
		context.close();
	}

	@Test
	void dlqProducerFactoryDestroyedForPolledConsumerOnUnbind() throws Exception {
		AtomicReference<DefaultKafkaProducerFactory<byte[], byte[]>> capturedFactory = new AtomicReference<>();
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		KafkaMessageChannelBinder binder = createDlqUnitTestBinder("dlq-polled-topic", "dlq-polled-group",
				capturedFactory, context);

		DefaultPollableMessageSource source = new DefaultPollableMessageSource(new StringMessageConverter());
		Binding<PollableSource<MessageHandler>> binding = binder.bindPollableConsumer("dlq-polled-topic",
				"dlq-polled-group", source, dlqConsumerProperties());

		DefaultKafkaProducerFactory<byte[], byte[]> dlqProducerFactory = capturedFactory.get();
		assertThat(dlqProducerFactory).isNotNull();
		verify(dlqProducerFactory, never()).destroy();

		binding.unbind();
		verify(dlqProducerFactory, times(1)).destroy();
		context.close();
	}

	private ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties() {
		KafkaConsumerProperties extension = new KafkaConsumerProperties();
		extension.setEnableDlq(true);
		extension.setAutoRebalanceEnabled(false);
		ExtendedConsumerProperties<KafkaConsumerProperties> properties = new ExtendedConsumerProperties<>(extension);
		properties.setInstanceCount(1);
		return properties;
	}

	@SuppressWarnings("unchecked")
	private KafkaMessageChannelBinder createDlqUnitTestBinder(String topic, String group,
			AtomicReference<DefaultKafkaProducerFactory<byte[], byte[]>> capturedFactory,
			GenericApplicationContext context) throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
				new TestKafkaProperties(), mock(ObjectProvider.class));
		KafkaTopicProvisioner provisioningProvider = mock(KafkaTopicProvisioner.class);
		ConsumerDestination dest = mock(ConsumerDestination.class);
		given(dest.getName()).willReturn(topic);
		given(provisioningProvider.provisionConsumerDestination(anyString(), anyString(), any())).willReturn(dest);
		List<PartitionInfo> partitionInfos = List.of(new PartitionInfo(topic, 0, null, null, null));
		given(provisioningProvider.getListenedPartitions(anyString(), any(), any(), anyInt(), anyBoolean(),
				anyBoolean(), anyString(), any())).willReturn(partitionInfos);
		given(provisioningProvider.getPartitionInfoForConsumer(anyString(), any(), any(), anyInt()))
				.willReturn(partitionInfos);

		Consumer<byte[], byte[]> consumer = mock(Consumer.class);
		willAnswer(i -> new ConsumerRecords<>(Collections.emptyMap())).given(consumer).poll(any(Duration.class));

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties, provisioningProvider,
				(container, destinationName, grp) -> container.getContainerProperties().setMissingTopicsFatal(false),
				null) {

			@Override
			protected DefaultKafkaProducerFactory<byte[], byte[]> getProducerFactory(String transactionIdPrefix,
					ExtendedProducerProperties<KafkaProducerProperties> producerProperties, String beanName,
					String destination) {
				DefaultKafkaProducerFactory<byte[], byte[]> factory = spy(
						super.getProducerFactory(transactionIdPrefix, producerProperties, beanName, destination));
				capturedFactory.set(factory);
				return factory;
			}

			@Override
			protected ConsumerFactory<?, ?> createKafkaConsumerFactory(boolean anonymous, String consumerGroup,
					ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties, String beanName,
					String destination) {
				ConsumerFactory<byte[], byte[]> consumerFactory = mock(ConsumerFactory.class);
				given(consumerFactory.createConsumer(any(), any(), any(), any())).willReturn(consumer);
				given(consumerFactory.createConsumer(any(), any(), any())).willReturn(consumer);
				given(consumerFactory.createConsumer(any(), any())).willReturn(consumer);
				given(consumerFactory.createConsumer(any())).willReturn(consumer);
				given(consumerFactory.createConsumer()).willReturn(consumer);
				given(consumerFactory.getConfigurationProperties()).willReturn(Map.of(
						ConsumerConfig.GROUP_ID_CONFIG, group,
						ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1));
				return consumerFactory;
			}

		};
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		return binder;
	}

}
