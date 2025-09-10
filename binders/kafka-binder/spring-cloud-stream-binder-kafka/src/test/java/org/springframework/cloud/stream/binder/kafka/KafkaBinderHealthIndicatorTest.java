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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.cloud.stream.binder.kafka.common.TopicInformation;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

/**
 * @author Barry Commins
 * @author Gary Russell
 * @author Laur Aliste
 * @author Soby Chacko
 * @author Chukwubuikem Ume-Ugwa
 * @author Taras Danylchuk
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaBinderHealthIndicatorTest {

	private static final String TEST_TOPIC = "test";

	private static final String REGEX_TOPIC = "regex*";

	private KafkaBinderHealthIndicator indicator;

	@Mock
	private DefaultKafkaConsumerFactory consumerFactory;

	@Mock
	private KafkaConsumer consumer;

	@Mock
	AbstractMessageListenerContainer<?, ?> listenerContainerA;

	@Mock
	AbstractMessageListenerContainer<?, ?> listenerContainerB;

	@Mock
	private KafkaMessageChannelBinder binder;

	private final Map<String, TopicInformation> topicsInUse = new HashMap<>();

	@BeforeEach
	public void setup() {
		MockitoAnnotations.initMocks(this);
		org.mockito.BDDMockito.given(consumerFactory.createConsumer())
				.willReturn(consumer);
		org.mockito.BDDMockito.given(binder.getTopicsInUse()).willReturn(topicsInUse);
		this.indicator = new KafkaBinderHealthIndicator(binder, consumerFactory);
		this.indicator.setTimeout(10);
	}

	@Test
	void kafkaBinderIsUpWithNoConsumers() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"group1-healthIndicator", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		org.mockito.BDDMockito.given(binder.getKafkaMessageListenerContainers())
				.willReturn(Collections.emptyList());

		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UP);
		assertThat(health.getDetails()).containsEntry("topicsInUse", singleton(TEST_TOPIC));
	}

	@Test
	void kafkaBinderIsUp() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"group1-healthIndicator", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		org.mockito.BDDMockito.given(binder.getKafkaMessageListenerContainers())
				.willReturn(Arrays.asList(listenerContainerA, listenerContainerB));
		mockContainer(listenerContainerA, true, true);
		mockContainer(listenerContainerB, true, true);

		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UP);
		assertThat(health.getDetails()).containsEntry("topicsInUse", singleton(TEST_TOPIC));
		assertThat(health.getDetails()).hasEntrySatisfying("listenerContainers", value ->
				assertThat((ArrayList<?>) value).hasSize(2));
	}

	@Test
	void kafkaBinderIsDownWhenOneOfConsumersIsNotRunning() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"group1-healthIndicator", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		org.mockito.BDDMockito.given(binder.getKafkaMessageListenerContainers())
				.willReturn(Arrays.asList(listenerContainerA, listenerContainerB));
		mockContainer(listenerContainerA, false, true);
		mockContainer(listenerContainerB, true, true);

		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UP);
		assertThat(health.getDetails()).containsEntry("topicsInUse", singleton(TEST_TOPIC));
		assertThat(health.getDetails()).hasEntrySatisfying("listenerContainers", value ->
				assertThat((ArrayList<?>) value).hasSize(2));
	}

	@Test
	void kafkaBinderIsDownWhenOneOfContainersWasStoppedAbnormally() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"group1-healthIndicator", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		org.mockito.BDDMockito.given(binder.getKafkaMessageListenerContainers())
				.willReturn(Arrays.asList(listenerContainerA, listenerContainerB));
		mockContainer(listenerContainerA, false, false);
		mockContainer(listenerContainerB, true, true);

		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		assertThat(health.getDetails()).containsEntry("topicsInUse", singleton(TEST_TOPIC));
		assertThat(health.getDetails()).hasEntrySatisfying("listenerContainers", value ->
				assertThat((ArrayList<?>) value).hasSize(2));
	}

	private void mockContainer(AbstractMessageListenerContainer<?, ?> container, boolean isRunning,
			boolean normalState) {

		org.mockito.BDDMockito.given(container.isRunning()).willReturn(isRunning);
		org.mockito.BDDMockito.given(container.isContainerPaused()).willReturn(true);
		org.mockito.BDDMockito.given(container.getListenerId()).willReturn("someListenerId");
		org.mockito.BDDMockito.given(container.getGroupId()).willReturn("someGroupId");
		org.mockito.BDDMockito.given(container.isInExpectedState()).willReturn(normalState);
	}

	@Test
	void kafkaBinderIsUpWithRegexTopic() {
		topicsInUse.put(REGEX_TOPIC, new TopicInformation(
				"regex-healthIndicator", null, true));
		Health health = indicator.health();
		// verify no consumer interaction for retrieving partitions
		org.mockito.BDDMockito.verify(consumer, Mockito.never())
				.partitionsFor(REGEX_TOPIC);
		// Ensuring the normal health check returns with status "up"
		assertThat(health.getStatus()).isEqualTo(Status.UP);
	}

	@Test
	void downWhenListTopicsThrowExceptionWithRegexTopic() {
		topicsInUse.put(REGEX_TOPIC, new TopicInformation(
			"regex-healthIndicator", null, true));
		org.mockito.BDDMockito.given(consumer.listTopics(any(Duration.class)))
			.willThrow(new IllegalStateException());

		Health health = indicator.health();

		// Ensuring the normal health check returns with status "up"
		assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		Map<String, Object> details = health.getDetails();
		assertThat(details.containsKey("Cluster not connected")).isTrue();
		assertThat(details
			.containsValue("Destination provided is a pattern, but cannot connect to the cluster for any verification")).isTrue();
	}


	@Test
	void kafkaBinderIsDown() {
		final List<PartitionInfo> partitions = partitions(null);
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"group2-healthIndicator", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.DOWN);
	}

	@Test
	void kafkaBinderIsDownWhenConsiderDownWhenAnyPartitionHasNoLeaderIsTrue() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		partitions.add(new PartitionInfo(TEST_TOPIC, 0, null, null, null));
		indicator.setConsiderDownWhenAnyPartitionHasNoLeader(true);
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"group2-healthIndicator", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.DOWN);
	}

	@Test
	void kafkaBinderIsUpWhenConsiderDownWhenAnyPartitionHasNoLeaderIsFalse() {
		Node node = new Node(0, null, 0);
		final List<PartitionInfo> partitions = partitions(node);
		partitions.add(new PartitionInfo(TEST_TOPIC, 0, null, null, null));
		indicator.setConsiderDownWhenAnyPartitionHasNoLeader(false);
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"group2-healthIndicator", partitions(node), false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);
		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UP);
	}

	@Test
	@Timeout(5)
	void kafkaBinderDoesNotAnswer() {
		final List<PartitionInfo> partitions = partitions(new Node(-1, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"group3-healthIndicator", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willAnswer(invocation -> {
					final int fiveMinutes = 1000 * 60 * 5;
					Thread.sleep(fiveMinutes);
					return partitions;
				});
		this.indicator.setTimeout(1);
		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.DOWN);
	}

	@Test
	void createsConsumerOnceWhenInvokedMultipleTimes() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"group4-healthIndicator", partitions, false));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC))
				.willReturn(partitions);

		indicator.health();
		Health health = indicator.health();

		assertThat(health.getStatus()).isEqualTo(Status.UP);
		org.mockito.Mockito.verify(this.consumerFactory).createConsumer();
	}

	@Test
	void consumerCreationFailsFirstTime() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new TopicInformation(
				"foo-healthIndicator", partitions, false));

		org.mockito.BDDMockito.given(consumerFactory.createConsumer())
				.willThrow(KafkaException.class).willReturn(consumer);

		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.DOWN);

		health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UP);

		org.mockito.Mockito.verify(this.consumerFactory, Mockito.times(2))
				.createConsumer();
	}

	@Test
	void testIfNoTopicsRegisteredByTheBinderProvidesDownStatus() {
		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UNKNOWN);
	}

	private List<PartitionInfo> partitions(Node leader) {
		List<PartitionInfo> partitions = new ArrayList<>();
		partitions.add(new PartitionInfo(TEST_TOPIC, 0, leader, null, null));
		return partitions;
	}
}
