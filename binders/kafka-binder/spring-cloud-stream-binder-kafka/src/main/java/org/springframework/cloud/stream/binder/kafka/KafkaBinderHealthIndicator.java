/*
 * Copyright 2016-present the original author or authors.
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.cloud.stream.binder.kafka.common.AbstractKafkaBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kafka.common.TopicInformation;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
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
 * @author Taras Danylchuk
 */
public class KafkaBinderHealthIndicator extends AbstractKafkaBinderHealthIndicator {

	private final KafkaMessageChannelBinder binder;


	public KafkaBinderHealthIndicator(KafkaMessageChannelBinder binder,
									ConsumerFactory<?, ?> consumerFactory) {
		super(consumerFactory);
		this.binder = binder;
	}

	@Override
	protected ExecutorService createHealthBinderExecutorService() {
		return Executors.newSingleThreadExecutor(
			new CustomizableThreadFactory("kafka-binder-health-"));
	}

	@Override
	protected Map<String, TopicInformation> getTopicsInUse() {
		return this.binder.getTopicsInUse();
	}

	@Override
	protected Health buildBinderSpecificHealthDetails() {
		List<AbstractMessageListenerContainer<?, ?>> listenerContainers = binder.getKafkaMessageListenerContainers();
		if (listenerContainers.isEmpty()) {
			return Health.unknown().build();
		}

		Status status = Status.UP;
		List<Map<String, Object>> containersDetails = new ArrayList<>();

		for (AbstractMessageListenerContainer<?, ?> container : listenerContainers) {
			Map<String, Object> containerDetails = new HashMap<>();
			boolean isRunning = container.isRunning();
			boolean isOk = container.isInExpectedState();
			if (!isOk) {
				status = Status.DOWN;
			}
			containerDetails.put("isRunning", isRunning);
			containerDetails.put("isStoppedAbnormally", !isRunning && !isOk);
			containerDetails.put("isPaused", container.isContainerPaused());
			containerDetails.put("listenerId", container.getListenerId());
			containerDetails.put("groupId", container.getGroupId());

			containersDetails.add(containerDetails);
		}
		return Health.status(status)
				.withDetail("listenerContainers", containersDetails)
				.build();
	}
}
