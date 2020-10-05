/*
 * Copyright 2016-2018 the original author or authors.
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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.springframework.cloud.stream.binder.BindingCreatedEvent;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Metrics for Kafka binder.
 *
 * @author Henryk Konsek
 * @author Soby Chacko
 * @author Artem Bilan
 * @author Oleg Zhurakousky
 * @author Jon Schneider
 * @author Thomas Cheyney
 * @author Gary Russell
 */
public class KafkaBinderMetrics
		implements MeterBinder, ApplicationListener<BindingCreatedEvent> {

	private static final int DEFAULT_TIMEOUT = 5;

	private static final int DELAY_BETWEEN_TASK_EXECUTION = 60;

	private static final Log LOG = LogFactory.getLog(KafkaBinderMetrics.class);

	static final String METRIC_NAME = "spring.cloud.stream.binder.kafka.offset";

	private final KafkaMessageChannelBinder binder;

	private final KafkaBinderConfigurationProperties binderConfigurationProperties;

	private ConsumerFactory<?, ?> defaultConsumerFactory;

	private final MeterRegistry meterRegistry;

	private Map<String, Consumer<?, ?>> metadataConsumers;

	private int timeout = DEFAULT_TIMEOUT;

	ScheduledExecutorService scheduler;

	Map<String, Long> unconsumedMessages = new ConcurrentHashMap<>();

	public KafkaBinderMetrics(KafkaMessageChannelBinder binder,
			KafkaBinderConfigurationProperties binderConfigurationProperties,
			ConsumerFactory<?, ?> defaultConsumerFactory,
			@Nullable MeterRegistry meterRegistry) {

		this.binder = binder;
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.defaultConsumerFactory = defaultConsumerFactory;
		this.meterRegistry = meterRegistry;
		this.metadataConsumers = new ConcurrentHashMap<>();
	}

	public KafkaBinderMetrics(KafkaMessageChannelBinder binder,
			KafkaBinderConfigurationProperties binderConfigurationProperties) {

		this(binder, binderConfigurationProperties, null, null);
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	@Override
	public void bindTo(MeterRegistry registry) {

		this.scheduler = Executors.newScheduledThreadPool(this.binder.getTopicsInUse().size());

		for (Map.Entry<String, KafkaMessageChannelBinder.TopicInformation> topicInfo : this.binder
				.getTopicsInUse().entrySet()) {

			if (!topicInfo.getValue().isConsumerTopic()) {
				continue;
			}

			String topic = topicInfo.getKey();
			String group = topicInfo.getValue().getConsumerGroup();

			//Schedule a task to compute the unconsumed messages for this group/topic every minute.
			this.scheduler.scheduleWithFixedDelay(computeUnconsumedMessagesRunnable(topic, group, this.metadataConsumers),
					10, DELAY_BETWEEN_TASK_EXECUTION, TimeUnit.SECONDS);

			Gauge.builder(METRIC_NAME, this,
					(o) -> computeAndGetUnconsumedMessages(topic, group)).tag("group", group)
					.tag("topic", topic)
					.description("Unconsumed messages for a particular group and topic")
					.register(registry);
		}
	}

	private Runnable computeUnconsumedMessagesRunnable(String topic, String group, Map<String, Consumer<?, ?>> metadataConsumers) {
		return () -> {
			try {
				long lag = findTotalTopicGroupLag(topic, group, this.metadataConsumers);
				this.unconsumedMessages.put(topic + "-" + group, lag);
			}
			catch (Exception ex) {
				LOG.debug("Cannot generate metric for topic: " + topic, ex);
			}
		};
	}

	private long computeAndGetUnconsumedMessages(String topic, String group) {
		ExecutorService exec = Executors.newCachedThreadPool();
		Future<Long> future = exec.submit(() -> {

			long lag = 0;
			try {
				lag = findTotalTopicGroupLag(topic, group, this.metadataConsumers);
			}
			catch (Exception ex) {
				LOG.debug("Cannot generate metric for topic: " + topic, ex);
			}
			return lag;
		});
		try {
			return future.get(this.timeout, TimeUnit.SECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return this.unconsumedMessages.getOrDefault(topic + "-" + group, 0L);
		}
		catch (ExecutionException | TimeoutException ex) {
			return this.unconsumedMessages.getOrDefault(topic + "-" + group, 0L);
		}
		finally {
			exec.shutdownNow();
		}
	}

	private long findTotalTopicGroupLag(String topic, String group, Map<String, Consumer<?, ?>> metadataConsumers) {
		long lag = 0;
		Consumer<?, ?> metadataConsumer = metadataConsumers.computeIfAbsent(
				group,
				(g) -> createConsumerFactory().createConsumer(g, "monitoring"));
		List<PartitionInfo> partitionInfos = metadataConsumer
				.partitionsFor(topic);
		List<TopicPartition> topicPartitions = new LinkedList<>();
		for (PartitionInfo partitionInfo : partitionInfos) {
			topicPartitions.add(new TopicPartition(partitionInfo.topic(),
					partitionInfo.partition()));
		}

		Map<TopicPartition, Long> endOffsets = metadataConsumer
				.endOffsets(topicPartitions);

		for (Map.Entry<TopicPartition, Long> endOffset : endOffsets
				.entrySet()) {
			OffsetAndMetadata current = metadataConsumer
					.committed(endOffset.getKey());
			lag += endOffset.getValue();
			if (current != null) {
				lag -= current.offset();
			}
		}
		return lag;
	}

	private synchronized  ConsumerFactory<?, ?> createConsumerFactory() {
		if (this.defaultConsumerFactory == null) {
			Map<String, Object> props = new HashMap<>();
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					ByteArrayDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					ByteArrayDeserializer.class);
			Map<String, Object> mergedConfig = this.binderConfigurationProperties
					.mergedConsumerConfiguration();
			if (!ObjectUtils.isEmpty(mergedConfig)) {
				props.putAll(mergedConfig);
			}
			if (!props.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
				props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
						this.binderConfigurationProperties
								.getKafkaConnectionString());
			}
			this.defaultConsumerFactory = new DefaultKafkaConsumerFactory<>(
					props);
		}
		return this.defaultConsumerFactory;
	}

	@Override
	public void onApplicationEvent(BindingCreatedEvent event) {
		if (this.meterRegistry != null) {
			// It is safe to call bindTo multiple times, since meters are idempotent when called with the same arguments
			this.bindTo(this.meterRegistry);
		}
	}

}
