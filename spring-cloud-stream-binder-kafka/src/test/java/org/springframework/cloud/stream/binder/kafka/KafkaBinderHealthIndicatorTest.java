/*
 * Copyright 2017 the original author or authors.
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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Barry Commins
 * @author Gary Russell
 * @author Laur Aliste
 */
public class KafkaBinderHealthIndicatorTest {

	private static final String TEST_TOPIC = "test";

	private KafkaBinderHealthIndicator indicator;

	@Mock
	private DefaultKafkaConsumerFactory consumerFactory;

	@Mock
	private KafkaConsumer consumer;

	@Mock
	private KafkaMessageChannelBinder binder;

	private final Map<String, KafkaMessageChannelBinder.TopicInformation> topicsInUse = new HashMap<>();

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		org.mockito.BDDMockito.given(consumerFactory.createConsumer()).willReturn(consumer);
		org.mockito.BDDMockito.given(binder.getTopicsInUse()).willReturn(topicsInUse);
		this.indicator = new KafkaBinderHealthIndicator(binder, consumerFactory);
		this.indicator.setTimeout(10);
	}

	@Test
	public void kafkaBinderIsUp() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new KafkaMessageChannelBinder.TopicInformation("group", partitions));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC)).willReturn(partitions);
		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UP);
	}

	@Test
	public void kafkaBinderIsDown() {
		final List<PartitionInfo> partitions = partitions(new Node(-1, null, 0));
		topicsInUse.put(TEST_TOPIC, new KafkaMessageChannelBinder.TopicInformation("group", partitions));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC)).willReturn(partitions);
		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.DOWN);
	}

	@Test(timeout = 5000)
	public void kafkaBinderDoesNotAnswer() {
		final List<PartitionInfo> partitions = partitions(new Node(-1, null, 0));
		topicsInUse.put(TEST_TOPIC, new KafkaMessageChannelBinder.TopicInformation("group", partitions));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC)).willAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				final int fiveMinutes = 1000 * 60 * 5;
				Thread.sleep(fiveMinutes);
				return partitions;
			}

		});
		this.indicator.setTimeout(1);
		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.DOWN);
	}

	@Test
	public void createsConsumerOnceWhenInvokedMultipleTimes() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new KafkaMessageChannelBinder.TopicInformation("group", partitions));
		org.mockito.BDDMockito.given(consumer.partitionsFor(TEST_TOPIC)).willReturn(partitions);

		indicator.health();
		Health health = indicator.health();

		assertThat(health.getStatus()).isEqualTo(Status.UP);
		org.mockito.Mockito.verify(this.consumerFactory).createConsumer();
	}

	@Test
	public void consumerCreationFailsFirstTime() {
		org.mockito.BDDMockito.given(consumerFactory.createConsumer()).willThrow(KafkaException.class)
				.willReturn(consumer);

		Health health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.DOWN);

		health = indicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UP);

		org.mockito.Mockito.verify(this.consumerFactory, Mockito.times(2)).createConsumer();
	}

	private List<PartitionInfo> partitions(Node leader) {
		List<PartitionInfo> partitions = new ArrayList<>();
		partitions.add(new PartitionInfo(TEST_TOPIC, 0, leader, null, null));
		return partitions;
	}
}
