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

	private static final int DEFAULT_TIMEOUT = 60;

	private static final Log LOG = LogFactory.getLog(KafkaBinderMetrics.class);

	static final String METRIC_NAME = "spring.cloud.stream.binder.kafka.offset";

	private final KafkaMessageChannelBinder binder;

	private final KafkaBinderConfigurationProperties binderConfigurationProperties;

	private ConsumerFactory<?, ?> defaultConsumerFactory;

	private final MeterRegistry meterRegistry;

	private Map<String, Consumer<?, ?>> metadataConsumers;

	private int timeout = DEFAULT_TIMEOUT;

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
		for (Map.Entry<String, KafkaMessageChannelBinder.TopicInformation> topicInfo : this.binder
				.getTopicsInUse().entrySet()) {

			if (!topicInfo.getValue().isConsumerTopic()) {
				continue;
			}

			String topic = topicInfo.getKey();
			String group = topicInfo.getValue().getConsumerGroup();

			Gauge.builder(METRIC_NAME, this,
					(o) -> computeUnconsumedMessages(topic, group)).tag("group", group)
					.tag("topic", topic)
					.description("Unconsumed messages for a particular group and topic")
					.register(registry);
		}
	}

	private long computeUnconsumedMessages(String topic, String group) {
		ExecutorService exec = Executors.newSingleThreadExecutor();
		Future<Long> future = exec.submit(() -> {

			long lag = 0;
			try {
				Consumer<?, ?> metadataConsumer = this.metadataConsumers.computeIfAbsent(
						group,
						(g) -> createConsumerFactory().createConsumer(g, "monitoring"));
				synchronized (metadataConsumer) {
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
				}
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
			return 0L;
		}
		catch (ExecutionException | TimeoutException ex) {
			return 0L;
		}
		finally {
			exec.shutdownNow();
		}
	}

	private ConsumerFactory<?, ?> createConsumerFactory() {
		if (this.defaultConsumerFactory == null) {
			synchronized (this) {
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
			}
		}

		return this.defaultConsumerFactory;
	}

	@Override
	public void onApplicationEvent(BindingCreatedEvent event) {
		if (this.meterRegistry != null) {
			// meters are idempotent when called with the same arguments so safe to call
			// it multiple times
			this.bindTo(this.meterRegistry);
		}
	}

}
