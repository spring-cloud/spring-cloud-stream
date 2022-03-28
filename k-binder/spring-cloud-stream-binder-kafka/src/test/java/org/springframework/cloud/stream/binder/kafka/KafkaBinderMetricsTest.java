/*
 * Copyright 2016-2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder.TopicInformation;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Henryk Konsek
 * @author Thomas Cheyney
 * @author Soby Chacko
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
		MockitoAnnotations.openMocks(this);
		org.mockito.BDDMockito.given(consumerFactory
				.createConsumer(ArgumentMatchers.any(), ArgumentMatchers.any()))
				.willReturn(consumer);
		org.mockito.BDDMockito.given(binder.getTopicsInUse()).willReturn(topicsInUse);
		metrics = new KafkaBinderMetrics(binder, kafkaBinderConfigurationProperties,
				consumerFactory, null);
		org.mockito.BDDMockito
				.given(consumer.endOffsets(ArgumentMatchers.anyCollection()))
				.willReturn(java.util.Collections
						.singletonMap(new TopicPartition(TEST_TOPIC, 0), 1000L));
	}

	@Test
	public void shouldIndicateLag() {
		final Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
		TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, 0);
		committed.put(topicPartition, new OffsetAndMetadata(500));
		org.mockito.BDDMockito
				.given(consumer.committed(ArgumentMatchers.anySet()))
				.willReturn(committed);
		List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC,
				new TopicInformation("group1-metrics", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		metrics.bindTo(meterRegistry);
		assertThat(meterRegistry.getMeters()).hasSize(1);
		assertThat(meterRegistry.get(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME)
				.tag("group", "group1-metrics").tag("topic", TEST_TOPIC).gauge().value())
						.isEqualTo(500.0);
	}

	@Test
	public void shouldNotContainAnyMetricsWhenUsingNoopGauge() {
		// Adding NoopGauge for the offset metric.
		meterRegistry.config().meterFilter(
				MeterFilter.denyNameStartsWith("spring.cloud.stream.binder.kafka.offset"));

		// Because we have NoopGauge for the offset metric  in the meter registry, none of these expectations matter.
		org.mockito.BDDMockito
				.given(consumer.committed(ArgumentMatchers.any(TopicPartition.class)))
				.willReturn(new OffsetAndMetadata(500));
		List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC,
				new TopicInformation("group1-metrics", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		metrics.bindTo(meterRegistry);

		// Because of the NoopGauge, the meterRegistry should contain no metric.
		assertThat(meterRegistry.getMeters()).hasSize(0);
	}

	@Test
	public void shouldSumUpPartitionsLags() {
		Map<TopicPartition, Long> endOffsets = new HashMap<>();
		endOffsets.put(new TopicPartition(TEST_TOPIC, 0), 1000L);
		endOffsets.put(new TopicPartition(TEST_TOPIC, 1), 1000L);
		org.mockito.BDDMockito
				.given(consumer.endOffsets(ArgumentMatchers.anyCollection()))
				.willReturn(endOffsets);
		final Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
		TopicPartition topicPartition1 = new TopicPartition(TEST_TOPIC, 0);
		TopicPartition topicPartition2 = new TopicPartition(TEST_TOPIC, 1);
		committed.put(topicPartition1, new OffsetAndMetadata(500));
		committed.put(topicPartition2, new OffsetAndMetadata(500));
		org.mockito.BDDMockito
				.given(consumer.committed(ArgumentMatchers.anySet()))
				.willReturn(committed);
		List<PartitionInfo> partitions = partitions(new Node(0, null, 0),
				new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC,
				new TopicInformation("group2-metrics", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		metrics.bindTo(meterRegistry);
		assertThat(meterRegistry.getMeters()).hasSize(1);
		assertThat(meterRegistry.get(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME)
				.tag("group", "group2-metrics").tag("topic", TEST_TOPIC).gauge().value())
						.isEqualTo(1000.0);
	}

	@Test
	public void shouldIndicateFullLagForNotCommittedGroups() {
		List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC,
				new TopicInformation("group3-metrics", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		metrics.bindTo(meterRegistry);
		assertThat(meterRegistry.getMeters()).hasSize(1);
		assertThat(meterRegistry.get(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME)
				.tag("group", "group3-metrics").tag("topic", TEST_TOPIC).gauge().value())
						.isEqualTo(1000.0);
	}

	@Test
	public void shouldNotCalculateLagForProducerTopics() {
		List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(null, partitions, false));
		metrics.bindTo(meterRegistry);
		assertThat(meterRegistry.getMeters()).isEmpty();
	}

	@Test
	public void createsConsumerOnceWhenInvokedMultipleTimes() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC,
				new TopicInformation("group4-metrics", partitions, false));

		metrics.bindTo(meterRegistry);

		Gauge gauge = meterRegistry.get(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME)
				.tag("group", "group4-metrics").tag("topic", TEST_TOPIC).gauge();
		gauge.value();
		assertThat(gauge.value()).isEqualTo(1000.0);

		org.mockito.Mockito.verify(this.consumerFactory)
				.createConsumer(ArgumentMatchers.any(), ArgumentMatchers.any());
	}

	@Test
	public void consumerCreationFailsFirstTime() {
		org.mockito.BDDMockito
				.given(consumerFactory.createConsumer(ArgumentMatchers.any(),
						ArgumentMatchers.any()))
				.willThrow(KafkaException.class).willReturn(consumer);

		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC,
				new TopicInformation("group5-metrics", partitions, false));

		metrics.bindTo(meterRegistry);

		Gauge gauge = meterRegistry.get(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME)
				.tag("group", "group5-metrics").tag("topic", TEST_TOPIC).gauge();
		assertThat(gauge.value()).isEqualTo(0);
		assertThat(gauge.value()).isEqualTo(1000.0);

		org.mockito.Mockito.verify(this.consumerFactory, Mockito.times(2))
				.createConsumer(ArgumentMatchers.any(), ArgumentMatchers.any());
	}

	@Test
	public void createOneConsumerPerGroup() {
		final List<PartitionInfo> partitions1 = partitions(new Node(0, null, 0));
		final List<PartitionInfo> partitions2 = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC,
				new TopicInformation("group1-metrics", partitions1, false));
		topicsInUse.put("test2",
				new TopicInformation("group2-metrics", partitions2, false));

		metrics.bindTo(meterRegistry);

		KafkaConsumer consumer2 = mock(KafkaConsumer.class);
		org.mockito.BDDMockito
				.given(consumerFactory.createConsumer(
						ArgumentMatchers.eq("group2-metrics"), ArgumentMatchers.any()))
				.willReturn(consumer2);
		org.mockito.BDDMockito
				.given(consumer2.endOffsets(ArgumentMatchers.anyCollection()))
				.willReturn(java.util.Collections
						.singletonMap(new TopicPartition("test2", 0), 50L));

		Gauge gauge1 = meterRegistry.get(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME)
				.tag("group", "group1-metrics").tag("topic", TEST_TOPIC).gauge();
		Gauge gauge2 = meterRegistry.get(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME)
				.tag("group", "group2-metrics").tag("topic", "test2").gauge();
		gauge1.value();
		gauge2.value();
		assertThat(gauge1.value()).isEqualTo(1000.0);
		assertThat(gauge2.value()).isEqualTo(50.0);

		org.mockito.Mockito.verify(this.consumerFactory).createConsumer(
				ArgumentMatchers.eq("group1-metrics"), ArgumentMatchers.any());
		org.mockito.Mockito.verify(this.consumerFactory).createConsumer(
				ArgumentMatchers.eq("group2-metrics"), ArgumentMatchers.any());
	}

	private List<PartitionInfo> partitions(Node... nodes) {
		List<PartitionInfo> partitions = new ArrayList<>();
		for (int i = 0; i < nodes.length; i++) {
			partitions.add(new PartitionInfo(TEST_TOPIC, i, nodes[i], null, null));
		}
		return partitions;
	}

}
