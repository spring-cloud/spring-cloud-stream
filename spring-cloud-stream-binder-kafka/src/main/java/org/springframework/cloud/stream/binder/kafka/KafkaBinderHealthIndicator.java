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

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Health indicator for Kafka.
 *
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Henryk Konsek
 * @author Gary Russell
 * @author Laur Aliste
 * @author Soby Chacko
 */
public class KafkaBinderHealthIndicator implements HealthIndicator {

	private static final int DEFAULT_TIMEOUT = 60;

	private final KafkaMessageChannelBinder binder;

	private final ConsumerFactory<?, ?> consumerFactory;

	private int timeout = DEFAULT_TIMEOUT;

	private Consumer<?, ?> metadataConsumer;

	public KafkaBinderHealthIndicator(KafkaMessageChannelBinder binder, ConsumerFactory<?, ?> consumerFactory) {
		this.binder = binder;
		this.consumerFactory = consumerFactory;
	}

	/**
	 * Set the timeout in seconds to retrieve health information.
	 *
	 * @param timeout the timeout - default 60.
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	@Override
	public Health health() {
		ExecutorService exec = Executors.newSingleThreadExecutor();
		Future<Health> future = exec.submit(() -> {
			try {
				if (metadataConsumer == null) {
					synchronized(KafkaBinderHealthIndicator.this) {
						if (metadataConsumer == null) {
							metadataConsumer = consumerFactory.createConsumer();
						}
					}
				}
				synchronized (metadataConsumer) {
					Set<String> downMessages = new HashSet<>();
					final Map<String, KafkaMessageChannelBinder.TopicInformation> topicsInUse =
							KafkaBinderHealthIndicator.this.binder.getTopicsInUse();
					for (String topic : topicsInUse.keySet()) {
						KafkaMessageChannelBinder.TopicInformation topicInformation = topicsInUse.get(topic);
						if (!topicInformation.isTopicPattern()) {
							List<PartitionInfo> partitionInfos = metadataConsumer.partitionsFor(topic);
							for (PartitionInfo partitionInfo : partitionInfos) {
								if (topicInformation.getPartitionInfos()
										.contains(partitionInfo) && partitionInfo.leader().id() == -1) {
									downMessages.add(partitionInfo.toString());
								}
							}
						}
					}
					if (downMessages.isEmpty()) {
						return Health.up().build();
					}
					else {
						return Health.down()
							.withDetail("Following partitions in use have no leaders: ", downMessages.toString())
							.build();
					}
				}
			}
			catch (Exception e) {
				return Health.down(e).build();
			}
		});
		try {
			return future.get(this.timeout, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return Health.down()
					.withDetail("Interrupted while waiting for partition information in", this.timeout + " seconds")
					.build();
		}
		catch (ExecutionException e) {
			return Health.down(e).build();
		}
		catch (TimeoutException e) {
			return Health.down()
					.withDetail("Failed to retrieve partition information in", this.timeout + " seconds")
					.build();
		}
		finally {
			exec.shutdownNow();
		}
	}

}
