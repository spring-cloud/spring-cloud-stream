/*
 * Copyright 2023-2023 the original author or authors.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.common.PartitionInfo;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cloud.stream.binder.kafka.common.AbstractKafkaBinderHealthIndicator;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

/**
 * {@link org.springframework.boot.actuate.health.HealthIndicator} for Reactor Kafka Binder.
 *
 * @author Soby Chacko
 */
public class ReactorKafkaBinderHealthIndicator extends AbstractKafkaBinderHealthIndicator {

	private final ReactorKafkaBinder binder;

	public ReactorKafkaBinderHealthIndicator(ReactorKafkaBinder binder,
											ConsumerFactory<?, ?> consumerFactory) {
		super(consumerFactory);
		this.binder = binder;
	}

	@Override
	protected ExecutorService createHealthBinderExecutorService() {
		return Executors.newSingleThreadExecutor(
			new CustomizableThreadFactory("reactor-kafka-binder-health-"));
	}

	protected Health buildTopicsHealth() {
		try {
			initMetadataConsumer();
			Set<String> downMessages = new HashSet<>();
			Set<String> checkedTopics = new HashSet<>();
			final Map<String, ReactorKafkaBinder.TopicInformation> topicsInUse = ReactorKafkaBinderHealthIndicator.this.binder
				.getTopicsInUse();
			if (topicsInUse.isEmpty()) {
				try {
					this.metadataConsumer.listTopics(Duration.ofSeconds(this.timeout));
				}
				catch (Exception e) {
					return Health.down().withDetail("No topic information available",
						"Kafka broker is not reachable").build();
				}
				return Health.unknown().withDetail("No bindings found",
					"Kafka binder may not be bound to destinations on the broker").build();
			}
			else {
				for (String topic : topicsInUse.keySet()) {
					ReactorKafkaBinder.TopicInformation topicInformation = topicsInUse
						.get(topic);
					if (!topicInformation.isTopicPattern()) {
						List<PartitionInfo> partitionInfos = this.metadataConsumer
							.partitionsFor(topic);
						for (PartitionInfo partitionInfo : partitionInfos) {
							if (topicInformation.getPartitionInfos()
								.contains(partitionInfo)
								&& partitionInfo.leader() == null ||
								(partitionInfo.leader() != null && partitionInfo.leader().id() == -1)) {
								downMessages.add(partitionInfo.toString());
							}
							else if (this.considerDownWhenAnyPartitionHasNoLeader &&
								partitionInfo.leader() == null || (partitionInfo.leader() != null && partitionInfo.leader().id() == -1)) {
								downMessages.add(partitionInfo.toString());
							}
						}
						checkedTopics.add(topic);
					}
					else {
						try {
							// Since destination is a pattern, all we are doing is just to make sure that
							// we can connect to the cluster and query the topics.
							this.metadataConsumer.listTopics(Duration.ofSeconds(this.timeout));
						}
						catch (Exception ex) {
							return Health.down()
								.withDetail("Cluster not connected",
									"Destination provided is a pattern, but cannot connect to the cluster for any verification")
								.build();
						}
					}
				}
			}
			if (downMessages.isEmpty()) {
				return Health.up().withDetail("topicsInUse", checkedTopics).build();
			}
			else {
				return Health.down()
					.withDetail("Following partitions in use have no leaders: ",
						downMessages.toString())
					.build();
			}
		}
		catch (Exception ex) {
			return Health.down(ex).build();
		}
	}

	protected Health buildBinderSpecificHealthDetails() {
		Map<String, MessageProducerSupport> messageProducerSupportInfo = binder.getMessageProducers();
		if (messageProducerSupportInfo.isEmpty()) {
			return Health.unknown().build();
		}

		Status status = Status.UP;
		List<Map<String, Object>> messageProducers = new ArrayList<>();

		Map<String, Object> messageProducerDetails = new HashMap<>();
		for (String groupId : messageProducerSupportInfo.keySet()) {
			MessageProducerSupport messageProducerSupport = messageProducerSupportInfo.get(groupId);
			boolean isRunning = messageProducerSupport.isRunning();
			boolean isOk = messageProducerSupport.isActive();
			if (!isOk) {
				status = Status.DOWN;
			}
			messageProducerDetails.put("isRunning", isRunning);
			messageProducerDetails.put("isStoppedAbnormally", !isRunning && !isOk);
			//messageProducerDetails.put("isPaused", messageProducerSupport.isPaused());
			messageProducerDetails.put("messageProducerId", messageProducerSupport.getApplicationContextId());
			messageProducerDetails.put("groupId", groupId);
		}
		messageProducers.add(messageProducerDetails);
		return Health.status(status)
			.withDetail("messageProducers", messageProducers)
			.build();
	}
}
