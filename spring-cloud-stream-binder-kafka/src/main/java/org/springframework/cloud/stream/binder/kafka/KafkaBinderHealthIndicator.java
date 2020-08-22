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

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

/**
 * Health indicator for Kafka.
 *
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Henryk Konsek
 * @author Gary Russell
 * @author Laur Aliste
 * @author Soby Chacko
 * @author Vladislav Fefelov
 * @author Chukwubuikem Ume-Ugwa
 */
public class KafkaBinderHealthIndicator implements HealthIndicator, DisposableBean {

	private static final int DEFAULT_TIMEOUT = 60;

	private final ExecutorService executor = Executors.newSingleThreadExecutor(
		new CustomizableThreadFactory("kafka-binder-health-"));

	private final KafkaMessageChannelBinder binder;

	private final ConsumerFactory<?, ?> consumerFactory;

	private int timeout = DEFAULT_TIMEOUT;

	private Consumer<?, ?> metadataConsumer;

	private boolean considerDownWhenAnyPartitionHasNoLeader;

	public KafkaBinderHealthIndicator(KafkaMessageChannelBinder binder,
			ConsumerFactory<?, ?> consumerFactory) {
		this.binder = binder;
		this.consumerFactory = consumerFactory;
	}

	/**
	 * Set the timeout in seconds to retrieve health information.
	 * @param timeout the timeout - default 60.
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setConsiderDownWhenAnyPartitionHasNoLeader(boolean considerDownWhenAnyPartitionHasNoLeader) {
		this.considerDownWhenAnyPartitionHasNoLeader = considerDownWhenAnyPartitionHasNoLeader;
	}

	@Override
	public Health health() {
		Future<Health> future = executor.submit(this::buildHealthStatus);
		try {
			return future.get(this.timeout, TimeUnit.SECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return Health.down()
					.withDetail("Interrupted while waiting for partition information in",
							this.timeout + " seconds")
					.build();
		}
		catch (ExecutionException ex) {
			return Health.down(ex).build();
		}
		catch (TimeoutException ex) {
			return Health.down().withDetail("Failed to retrieve partition information in",
					this.timeout + " seconds").build();
		}
	}

	private void initMetadataConsumer() {
		if (this.metadataConsumer == null) {
			this.metadataConsumer = this.consumerFactory.createConsumer();
		}
	}

	private Health buildHealthStatus() {
		try {
			initMetadataConsumer();
			Set<String> downMessages = new HashSet<>();
			final Map<String, KafkaMessageChannelBinder.TopicInformation> topicsInUse = KafkaBinderHealthIndicator.this.binder
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
					KafkaMessageChannelBinder.TopicInformation topicInformation = topicsInUse
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
					}
				}
			}
			if (downMessages.isEmpty()) {
				return Health.up().build();
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

	@Override
	public void destroy() throws Exception {
		executor.shutdown();
	}

}
