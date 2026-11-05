/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.common;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.health.actuate.endpoint.StatusAggregator;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.util.Assert;

/**
 * Base class that abstracts the common health indicator details for the various Kafka binder flavors.
 *
 * @author Soby Chacko
 * @since 4.1.0
 */
public abstract class AbstractKafkaBinderHealthIndicator extends AbstractHealthIndicator implements DisposableBean {

	private static final Log LOGGER = LogFactory.getLog(AbstractKafkaBinderHealthIndicator.class);

	private static final int DEFAULT_TIMEOUT = 60;

	protected int timeout = DEFAULT_TIMEOUT;

	private final ExecutorService executor;

	protected Consumer<?, ?> metadataConsumer;

	// if the binder detects that a partition for the topic
	// is without a leader, mark the binder health as DOWN.
	protected boolean considerDownWhenAnyPartitionHasNoLeader = true;

	private final ConsumerFactory<?, ?> consumerFactory;

	// the last health check failure that was logged at WARN level, so that repeated
	// health probes failing for the same reason do not flood the logs.
	private volatile String lastLoggedFailure;

	public AbstractKafkaBinderHealthIndicator(ConsumerFactory<?, ?> consumerFactory) {
		this.consumerFactory = consumerFactory;
		this.executor = createHealthBinderExecutorService();
		Assert.notNull(this.executor, "The health indicator executor service must not be null");
	}

	protected abstract Map<String, TopicInformation> getTopicsInUse();

	protected abstract Health buildBinderSpecificHealthDetails();

	protected abstract ExecutorService createHealthBinderExecutorService();

	private void initMetadataConsumer() {
		if (this.metadataConsumer == null) {
			this.metadataConsumer = this.consumerFactory.createConsumer();
		}
	}

	@Override
	public void destroy() {
		executor.shutdown();
		if (this.metadataConsumer != null) {
			this.metadataConsumer.close();
		}
	}

	@Override
	protected void doHealthCheck(Health.Builder builder) throws Exception {
		Health topicsHealth = safelyBuildTopicsHealth();
		Health listenerContainersHealth = buildBinderSpecificHealthDetails();
		merge(topicsHealth, listenerContainersHealth, builder);
	}

	protected Health safelyBuildTopicsHealth() {
		Future<Health> future = executor.submit(this::buildTopicsHealth);
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
			logFailure("Kafka binder health check failed", ex.getCause() != null ? ex.getCause() : ex);
			return Health.down(ex).build();
		}
		catch (TimeoutException ex) {
			return Health.down().withDetail("Failed to retrieve partition information in",
				this.timeout + " seconds").build();
		}
	}

	private Health buildTopicsHealth() {
		try {
			initMetadataConsumer();
			Set<String> downMessages = new HashSet<>();
			Set<String> checkedTopics = new HashSet<>();
			final Map<String, TopicInformation> topicsInUse = getTopicsInUse();
			if (topicsInUse.isEmpty()) {
				try {
					this.metadataConsumer.listTopics(Duration.ofSeconds(this.timeout));
				}
				catch (Exception e) {
					logFailure("Kafka binder health check failed to list topics; the Kafka broker may not be reachable", e);
					return Health.down().withDetail("No topic information available",
						"Kafka broker is not reachable").build();
				}
				this.lastLoggedFailure = null;
				return Health.unknown().withDetail("No bindings found",
					"Kafka binder may not be bound to destinations on the broker").build();
			}
			else {
				for (String topic : topicsInUse.keySet()) {
					TopicInformation topicInformation = topicsInUse
						.get(topic);
					if (!topicInformation.isTopicPattern()) {
						List<PartitionInfo> partitionInfos = this.metadataConsumer
							.partitionsFor(topic);
						for (PartitionInfo partitionInfo : partitionInfos) {
							if (topicInformation.partitionInfos()
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
							logFailure("Kafka binder health check failed to list topics for pattern destination '"
									+ topic + "'", ex);
							return Health.down()
								.withDetail("Cluster not connected",
									"Destination provided is a pattern, but cannot connect to the cluster for any verification")
								.build();
						}
					}
				}
			}
			this.lastLoggedFailure = null;
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
			logFailure("Kafka binder health check failed to retrieve topic information", ex);
			return Health.down(ex).build();
		}
	}

	/**
	 * Log a health check failure, including its cause chain. Health probes typically run
	 * at a regular interval, so a failure is only logged at WARN level when it differs
	 * from the previously logged one; repeats of the same failure are logged at DEBUG.
	 * @param message the log message
	 * @param ex the failure
	 */
	private void logFailure(String message, Throwable ex) {
		String failure = ex.getClass().getName() + ": " + ex.getMessage();
		if (!failure.equals(this.lastLoggedFailure)) {
			this.lastLoggedFailure = failure;
			LOGGER.warn(message, ex);
		}
		else if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(message, ex);
		}
	}

	private void merge(Health topicsHealth, Health listenerContainersHealth, Health.Builder builder) {
		Status aggregatedStatus = StatusAggregator.getDefault()
			.getAggregateStatus(topicsHealth.getStatus(), listenerContainersHealth.getStatus());
		Map<String, Object> aggregatedDetails = new HashMap<>();
		aggregatedDetails.putAll(topicsHealth.getDetails());
		aggregatedDetails.putAll(listenerContainersHealth.getDetails());
		builder.status(aggregatedStatus).withDetails(aggregatedDetails);
	}

	/**
	 * Set the timeout in seconds to retrieve health information.
	 *
	 * @param timeout the timeout - default 60.
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setConsiderDownWhenAnyPartitionHasNoLeader(boolean considerDownWhenAnyPartitionHasNoLeader) {
		this.considerDownWhenAnyPartitionHasNoLeader = considerDownWhenAnyPartitionHasNoLeader;
	}
}
