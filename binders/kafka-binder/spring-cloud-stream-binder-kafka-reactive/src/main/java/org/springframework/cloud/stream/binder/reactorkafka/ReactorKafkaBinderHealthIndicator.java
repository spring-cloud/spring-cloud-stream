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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cloud.stream.binder.kafka.common.AbstractKafkaBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kafka.common.TopicInformation;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

/**
 * {@link org.springframework.boot.actuate.health.HealthIndicator} for Reactor Kafka Binder.
 *
 * @author Soby Chacko
 *
 * @deprecated since 4.3
 * See the updates in: <a href="https://spring.io/blog/2025/05/20/reactor-kafka-discontinued">...</a>
 * A suggested alternative is to use the regular Kafka binder with reactive types.
 * This approach has some limitations as the application need to handle reactive use cases explicitly.
 */
@Deprecated(since = "4.3", forRemoval = true)
public class ReactorKafkaBinderHealthIndicator extends AbstractKafkaBinderHealthIndicator {

	private final ReactorKafkaBinder binder;

	public ReactorKafkaBinderHealthIndicator(ReactorKafkaBinder binder, ConsumerFactory<?, ?> consumerFactory) {
		super(consumerFactory);
		this.binder = binder;
	}

	@Override
	protected ExecutorService createHealthBinderExecutorService() {
		return Executors.newSingleThreadExecutor(
			new CustomizableThreadFactory("reactor-kafka-binder-health-"));
	}

	@Override
	protected Map<String, TopicInformation> getTopicsInUse() {
		return this.binder.getTopicsInUse();
	}

	@Override
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
