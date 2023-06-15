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

package org.springframework.cloud.stream.binder.kafka.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.Consumer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.actuate.health.StatusAggregator;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Base class that abstracts the common health indicator details for the various Kafka binder flavors.
 *
 * @author Soby Chacko
 */
public abstract class AbstractKafkaBinderHealthIndicator extends AbstractHealthIndicator implements DisposableBean {

	private static final int DEFAULT_TIMEOUT = 60;

	protected int timeout = DEFAULT_TIMEOUT;

	private final ExecutorService executor;

	protected Consumer<?, ?> metadataConsumer;

	protected boolean considerDownWhenAnyPartitionHasNoLeader;

	private final ConsumerFactory<?, ?> consumerFactory;

	public AbstractKafkaBinderHealthIndicator(ConsumerFactory<?, ?> consumerFactory) {
		this.consumerFactory = consumerFactory;
		this.executor = createHealthBinderExecutorService();
	}

	protected abstract Health buildTopicsHealth();

	protected abstract Health buildBinderSpecificHealthDetails();

	protected  abstract ExecutorService createHealthBinderExecutorService();

	protected void initMetadataConsumer() {
		if (this.metadataConsumer == null) {
			this.metadataConsumer = this.consumerFactory.createConsumer();
		}
	}

	@Override
	public void destroy() {
		executor.shutdown();
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
			return Health.down(ex).build();
		}
		catch (TimeoutException ex) {
			return Health.down().withDetail("Failed to retrieve partition information in",
				this.timeout + " seconds").build();
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
