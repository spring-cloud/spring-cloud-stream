/*
 * Copyright 2016-2018 the original author or authors.
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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.util.ObjectUtils;

/**
 * Metrics for Kafka binder.
 *
 * @author Henryk Konsek
 * @author Soby Chacko
 * @author Artem Bilan
 */
public class KafkaBinderMetrics implements MeterBinder {

	private final static Log LOG = LogFactory.getLog(KafkaBinderMetrics.class);

	static final String METRIC_PREFIX = "spring.cloud.stream.binder.kafka";

	private final KafkaMessageChannelBinder binder;

	private final KafkaBinderConfigurationProperties binderConfigurationProperties;

	private ConsumerFactory<?, ?> defaultConsumerFactory;

	public KafkaBinderMetrics(KafkaMessageChannelBinder binder,
			KafkaBinderConfigurationProperties binderConfigurationProperties,
			ConsumerFactory<?, ?> defaultConsumerFactory) {

		this.binder = binder;
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.defaultConsumerFactory = defaultConsumerFactory;
	}

	public KafkaBinderMetrics(KafkaMessageChannelBinder binder,
			KafkaBinderConfigurationProperties binderConfigurationProperties) {

		this(binder, binderConfigurationProperties, null);
	}

	@Override
	public void bindTo(MeterRegistry registry) {
		for (Map.Entry<String, KafkaMessageChannelBinder.TopicInformation> topicInfo : this.binder.getTopicsInUse()
				.entrySet()) {

			if (!topicInfo.getValue().isConsumerTopic()) {
				continue;
			}

			String topic = topicInfo.getKey();
			String group = topicInfo.getValue().getConsumerGroup();

			registry.gauge(String.format("%s.%s.%s.lag", METRIC_PREFIX, group, topic), this,
					o -> calculateConsumerLagOnTopic(topic, group));
		}
	}

	private double calculateConsumerLagOnTopic(String topic, String group) {
		long lag = 0;
		try (Consumer<?, ?> metadataConsumer = createConsumerFactory(group).createConsumer()) {
			List<PartitionInfo> partitionInfos = metadataConsumer.partitionsFor(topic);
			List<TopicPartition> topicPartitions = new LinkedList<>();
			for (PartitionInfo partitionInfo : partitionInfos) {
				topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
			}
			Map<TopicPartition, Long> endOffsets = metadataConsumer.endOffsets(topicPartitions);

			for (Map.Entry<TopicPartition, Long> endOffset : endOffsets.entrySet()) {
				OffsetAndMetadata current = metadataConsumer.committed(endOffset.getKey());
				if (current != null) {
					lag += endOffset.getValue() - current.offset();
				}
				else {
					lag += endOffset.getValue();
				}
			}
		}
		catch (Exception e) {
			LOG.debug("Cannot generate metric for topic: " + topic, e);
		}
		return lag;
	}

	private ConsumerFactory<?, ?> createConsumerFactory(String group) {
		if (this.defaultConsumerFactory == null) {
			Map<String, Object> props = new HashMap<>();
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			if (!ObjectUtils.isEmpty(binderConfigurationProperties.getConsumerConfiguration())) {
				props.putAll(binderConfigurationProperties.getConsumerConfiguration());
			}
			if (!props.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
				props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
						this.binderConfigurationProperties.getKafkaConnectionString());
			}
			props.put("group.id", group);
			this.defaultConsumerFactory = new DefaultKafkaConsumerFactory<>(props);
		}

		return this.defaultConsumerFactory;
	}

}
