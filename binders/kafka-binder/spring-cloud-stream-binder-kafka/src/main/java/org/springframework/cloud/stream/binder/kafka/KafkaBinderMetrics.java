/*
 * Copyright 2016-2024 the original author or authors.
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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.noop.NoopGauge;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.springframework.cloud.stream.binder.BindingCreatedEvent;
import org.springframework.cloud.stream.binder.kafka.common.TopicInformation;
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
 * @author Lars Bilger
 * @author Tomek Szmytka
 * @author Nico Heller
 * @author Kurt Hong
 * @author Omer Celik
 */
public class KafkaBinderMetrics
		implements MeterBinder, ApplicationListener<BindingCreatedEvent>, AutoCloseable {

	private static final int DEFAULT_TIMEOUT = 5;

	private static final Log LOG = LogFactory.getLog(KafkaBinderMetrics.class);

	/**
	 * Offset lag micrometer metric name. This can be used for meter filtering.
	 */
	public static final String OFFSET_LAG_METRIC_NAME = "spring.cloud.stream.binder.kafka.offset";

	private final KafkaMessageChannelBinder binder;

	private final KafkaBinderConfigurationProperties binderConfigurationProperties;

	private ConsumerFactory<?, ?> defaultConsumerFactory;

	private final MeterRegistry meterRegistry;

	private Map<String, Consumer<?, ?>> metadataConsumers;

	private int timeout = DEFAULT_TIMEOUT;

	private final Map<String, Long> lastUnconsumedMessagesValues = new ConcurrentHashMap<>();

	ScheduledExecutorService scheduler;

	private final ReentrantLock lock = new ReentrantLock();

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
		/**
		 * We can't just replace one scheduler with another.
		 * Before and even after the old one is gathered by GC, it's threads still exist, consume memory and CPU resources to switch contexts.
		 * Theoretically, as a result of processing n topics, there will be about (1+n)*n/2 threads simultaneously at the same time.
		 */
		if (this.scheduler != null) {
			LOG.info("Try to shutdown the old scheduler with " + ((ScheduledThreadPoolExecutor) scheduler).getPoolSize() + " threads");
			this.scheduler.shutdown();
		}

		this.scheduler = Executors.newScheduledThreadPool(this.binder.getTopicsInUse().size());

		for (Map.Entry<String, TopicInformation> topicInfo : this.binder
				.getTopicsInUse().entrySet()) {

			if (!topicInfo.getValue().isConsumerTopic()) {
				continue;
			}

			String topic = topicInfo.getKey();
			String group = topicInfo.getValue().consumerGroup();

			ToDoubleFunction<KafkaBinderMetrics> offsetComputation = computeOffsetComputationFunction(topic, group);
			final Gauge register = Gauge.builder(OFFSET_LAG_METRIC_NAME, this, offsetComputation)
				.tag("group", group)
				.tag("topic", topic)
				.description("Unconsumed messages for a particular group and topic")
				.register(registry);

			if (!(register instanceof NoopGauge)) {
				lastUnconsumedMessagesValues.put(topic + "-" + group, 0L);
				this.scheduler.scheduleWithFixedDelay(
					() -> computeUnconsumedMessages(topic, group),
			1,
					binderConfigurationProperties.getMetrics().getOffsetLagMetricsInterval().toSeconds(),
					TimeUnit.SECONDS
				);
			}
		}
	}

	private ToDoubleFunction<KafkaBinderMetrics> computeOffsetComputationFunction(String topic, String group) {
		if (this.binderConfigurationProperties.getMetrics().isDefaultOffsetLagMetricsEnabled()) {
			return (o) -> computeAndGetUnconsumedMessagesWithTimeout(topic, group);
		}
		else {
			return (o) -> lastUnconsumedMessagesValues.get(topic + "-" + group);
		}
	}

	private long computeAndGetUnconsumedMessagesWithTimeout(String topic, String group) {
		Future<Long> future = scheduler.submit(() -> computeUnconsumedMessages(topic, group));
		try {
			return future.get(this.timeout, TimeUnit.SECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return lastUnconsumedMessagesValues.get(topic + "-" + group);
		}
		catch (ExecutionException | TimeoutException ex) {
			return lastUnconsumedMessagesValues.get(topic + "-" + group);
		}
	}

	private long computeUnconsumedMessages(String topic, String group) {
		long lag = 0;
		try {
			lag = findTotalTopicGroupLag(topic, group, this.metadataConsumers);
			this.lastUnconsumedMessagesValues.put(topic + "-" + group, lag);
		}
		catch (Exception ex) {
			LOG.debug("Cannot generate metric for topic: " + topic, ex);
		}
		return lag;
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

		final Map<TopicPartition, OffsetAndMetadata> committedOffsets = metadataConsumer.committed(endOffsets.keySet());
		final Map<TopicPartition, Long> beginningOffsets = metadataConsumer.beginningOffsets(endOffsets.keySet());

		for (Map.Entry<TopicPartition, Long> endOffset : endOffsets
				.entrySet()) {
			OffsetAndMetadata current = committedOffsets.get(endOffset.getKey());
			Long beginningOffset = beginningOffsets.get(endOffset.getKey());
			lag += endOffset.getValue();
			if (current != null) {
				lag -= current.offset();
			}
			else if (beginningOffset != null) {
				lag -= beginningOffset;
			}
		}
		return lag;
	}

	private ConsumerFactory<?, ?> createConsumerFactory() {
		try {
			lock.lock();
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
		finally {
			lock.unlock();
		}
	}

	@Override
	public void onApplicationEvent(BindingCreatedEvent event) {
		if (this.meterRegistry != null) {
			// It is safe to call bindTo multiple times, since meters are idempotent when called with the same arguments
			this.bindTo(this.meterRegistry);
		}
	}

	@Override
	public void close() throws Exception {
		if (this.meterRegistry != null) {
			this.meterRegistry.find(OFFSET_LAG_METRIC_NAME).meters().forEach(this.meterRegistry::remove);
		}
		Optional.ofNullable(scheduler).ifPresent(ExecutorService::shutdown);
	}
}
