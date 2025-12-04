/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.cloud.stream.binder.reactorkafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cloud.stream.binder.kafka.common.TopicInformation;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReactorKafkaBinderHealthIndicatorTests {

	private static final String TEST_TOPIC = "test";

	private ReactorKafkaBinderHealthIndicator indicator;

	@Mock
	private DefaultKafkaConsumerFactory<?, ?> consumerFactory;

	@Mock
	private KafkaConsumer consumer;

	@Mock
	MessageProducerSupport messageProducerSupport1;

	@Mock
	private ReactorKafkaBinder binder;

	private final Map<String, TopicInformation> topicsInUse = new HashMap<>();

	@BeforeEach
	public void setup() {
		MockitoAnnotations.openMocks(this);
		org.mockito.BDDMockito.given(consumerFactory.createConsumer())
			.willReturn((consumer));
		org.mockito.BDDMockito.given(binder.getTopicsInUse()).willReturn(topicsInUse);
		this.indicator = new ReactorKafkaBinderHealthIndicator(binder, consumerFactory);
		this.indicator.setTimeout(10);
	}

	@Test
	void reactorKafkaBinderIsUp() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
			"group1-healthIndicator", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
			.willReturn(partitions);
		org.mockito.BDDMockito.given(binder.getMessageProducers())
			.willReturn(Map.of("group1-healthIndicator", messageProducerSupport1));
		org.mockito.BDDMockito.given(messageProducerSupport1.isRunning()).willReturn(true);
		org.mockito.BDDMockito.given(messageProducerSupport1.isActive()).willReturn(true);
		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UP);
		assertThat(health.getDetails()).containsEntry("topicsInUse", singleton(TEST_TOPIC));
		assertThat(health.getDetails()).hasEntrySatisfying("messageProducers", value ->
			assertThat((ArrayList<?>) value).hasSize(1));
	}

	private List<PartitionInfo> partitions(Node leader) {
		List<PartitionInfo> partitions = new ArrayList<>();
		partitions.add(new PartitionInfo(TEST_TOPIC, 0, leader, null, null));
		return partitions;
	}

}
