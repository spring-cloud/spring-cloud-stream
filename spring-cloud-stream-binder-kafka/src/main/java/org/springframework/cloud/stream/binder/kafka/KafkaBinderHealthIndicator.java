/*
 * Copyright 2016-2017 the original author or authors.
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
import java.util.Set;

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
 */
public class KafkaBinderHealthIndicator implements HealthIndicator {

	private final KafkaMessageChannelBinder binder;

	private final ConsumerFactory<?, ?> consumerFactory;

	public KafkaBinderHealthIndicator(KafkaMessageChannelBinder binder,
			ConsumerFactory<?, ?> consumerFactory) {
		this.binder = binder;
		this.consumerFactory = consumerFactory;

	}

	@Override
	public Health health() {
		try (Consumer<?, ?> metadataConsumer = consumerFactory.createConsumer()) {
			Set<String> downMessages = new HashSet<>();
			for (String topic : this.binder.getTopicsInUse().keySet()) {
				List<PartitionInfo> partitionInfos = metadataConsumer.partitionsFor(topic);
				for (PartitionInfo partitionInfo : partitionInfos) {
					if (this.binder.getTopicsInUse().get(topic).getPartitionInfos().contains(partitionInfo)
							&& partitionInfo.leader()
									.id() == -1) {
						downMessages.add(partitionInfo.toString());
					}
				}
			}
			if (downMessages.isEmpty()) {
				return Health.up().build();
			}
			return Health.down().withDetail("Following partitions in use have no leaders: ", downMessages.toString())
					.build();
		}
		catch (Exception e) {
			return Health.down(e).build();
		}
	}
}
