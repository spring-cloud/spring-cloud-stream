/*
 * Copyright 2016-2017 the original author or authors.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder.TopicInformation;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Henryk Konsek
 */
public class KafkaBinderMetricsTest {

	private static final String TEST_TOPIC = "test";

	private KafkaBinderMetrics metrics;

	@Mock
	private DefaultKafkaConsumerFactory consumerFactory;

	@Mock
	private KafkaConsumer consumer;

	@Mock
	private KafkaMessageChannelBinder binder;

	private MeterRegistry meterRegistry = new SimpleMeterRegistry();

	private Map<String, TopicInformation> topicsInUse = new HashMap<>();

	@Mock
	private KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		org.mockito.BDDMockito.given(consumerFactory.createConsumer()).willReturn(consumer);
		org.mockito.BDDMockito.given(binder.getTopicsInUse()).willReturn(topicsInUse);
		metrics = new KafkaBinderMetrics(binder, kafkaBinderConfigurationProperties, consumerFactory);
		org.mockito.BDDMockito.given(consumer.endOffsets(org.mockito.Matchers.anyCollectionOf(TopicPartition.class)))
				.willReturn(java.util.Collections.singletonMap(new TopicPartition(TEST_TOPIC, 0), 1000L));
	}

	@Test
	public void shouldIndicateLag() {
		org.mockito.BDDMockito.given(consumer.committed(org.mockito.Matchers.any(TopicPartition.class))).willReturn(new OffsetAndMetadata(500));
		List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation("group", partitions));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC)).willReturn(partitions);
		metrics.bindTo(meterRegistry);
		assertThat(meterRegistry.getMeters()).hasSize(1);
		Search group = meterRegistry.find(String.format("%s.%s.%s.lag", KafkaBinderMetrics.METRIC_PREFIX, "group", TEST_TOPIC));
		assertThat(group.gauge().value()).isEqualTo(500.0);
	}

	@Test
	public void shouldSumUpPartitionsLags() {
		Map<TopicPartition, Long> endOffsets = new HashMap<>();
		endOffsets.put(new TopicPartition(TEST_TOPIC, 0), 1000L);
		endOffsets.put(new TopicPartition(TEST_TOPIC, 1), 1000L);
		org.mockito.BDDMockito.given(consumer.endOffsets(org.mockito.Matchers.anyCollectionOf(TopicPartition.class))).willReturn(endOffsets);
		org.mockito.BDDMockito.given(consumer.committed(org.mockito.Matchers.any(TopicPartition.class))).willReturn(new OffsetAndMetadata(500));
		List<PartitionInfo> partitions = partitions(new Node(0, null, 0), new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation("group", partitions));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC)).willReturn(partitions);
		metrics.bindTo(meterRegistry);
		assertThat(meterRegistry.getMeters()).hasSize(1);
		Search group = meterRegistry.find(String.format("%s.%s.%s.lag", KafkaBinderMetrics.METRIC_PREFIX, "group", TEST_TOPIC));
		assertThat(group.gauge().value()).isEqualTo(1000.0);
	}

	@Test
	public void shouldIndicateFullLagForNotCommittedGroups() {
		List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation("group", partitions));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC)).willReturn(partitions);
		metrics.bindTo(meterRegistry);
		assertThat(meterRegistry.getMeters()).hasSize(1);
		Search group = meterRegistry.find(String.format("%s.%s.%s.lag", KafkaBinderMetrics.METRIC_PREFIX, "group", TEST_TOPIC));
		assertThat(group.gauge().value()).isEqualTo(1000.0);
	}

	@Test
	public void shouldNotCalculateLagForProducerTopics() {
		List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(null, partitions));
		metrics.bindTo(meterRegistry);
		assertThat(meterRegistry.getMeters()).isEmpty();
	}

	private List<PartitionInfo> partitions(Node... nodes) {
		List<PartitionInfo> partitions = new ArrayList<>();
		for (int i = 0; i < nodes.length; i++) {
			partitions.add(new PartitionInfo(TEST_TOPIC, i, nodes[i], null, null));
		}
		return partitions;
	}

}
