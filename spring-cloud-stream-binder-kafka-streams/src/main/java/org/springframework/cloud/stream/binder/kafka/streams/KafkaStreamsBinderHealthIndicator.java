/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Health indicator for Kafka Streams.
 *
 * @author Arnaud Jardiné
 * @author Soby Chacko
 */
public class KafkaStreamsBinderHealthIndicator extends AbstractHealthIndicator {

	private final KafkaStreamsRegistry kafkaStreamsRegistry;

	KafkaStreamsBinderHealthIndicator(KafkaStreamsRegistry kafkaStreamsRegistry) {
		super("Kafka-streams health check failed");
		this.kafkaStreamsRegistry = kafkaStreamsRegistry;
	}

	@Override
	protected void doHealthCheck(Health.Builder builder) throws Exception {
		boolean up = true;
		for (KafkaStreams kStream : kafkaStreamsRegistry.getKafkaStreams()) {
			up &= kStream.state().isRunning();
			builder.withDetails(buildDetails(kStream));
		}
		builder.status(up ? Status.UP : Status.DOWN);
	}

	private Map<String, Object> buildDetails(KafkaStreams kafkaStreams) {
		final Map<String, Object> details = new HashMap<>();
		final Map<String, Object> perAppdIdDetails = new HashMap<>();
		if (kafkaStreams.state().isRunning()) {
			for (ThreadMetadata metadata : kafkaStreams.localThreadsMetadata()) {
				perAppdIdDetails.put("threadName", metadata.threadName());
				perAppdIdDetails.put("threadState", metadata.threadState());
				perAppdIdDetails.put("adminClientId", metadata.adminClientId());
				perAppdIdDetails.put("consumerClientId", metadata.consumerClientId());
				perAppdIdDetails.put("restoreConsumerClientId", metadata.restoreConsumerClientId());
				perAppdIdDetails.put("producerClientIds", metadata.producerClientIds());
				perAppdIdDetails.put("activeTasks", taskDetails(metadata.activeTasks()));
				perAppdIdDetails.put("standbyTasks", taskDetails(metadata.standbyTasks()));
			}
			final StreamsBuilderFactoryBean streamsBuilderFactoryBean = this.kafkaStreamsRegistry.streamBuilderFactoryBean(kafkaStreams);
			final String applicationId = (String) streamsBuilderFactoryBean.getStreamsConfiguration().get(StreamsConfig.APPLICATION_ID_CONFIG);
			details.put(applicationId, perAppdIdDetails);
		}
		else {
			final StreamsBuilderFactoryBean streamsBuilderFactoryBean = this.kafkaStreamsRegistry.streamBuilderFactoryBean(kafkaStreams);
			final String applicationId = (String) streamsBuilderFactoryBean.getStreamsConfiguration().get(StreamsConfig.APPLICATION_ID_CONFIG);
			details.put(applicationId, String.format("The processor with application.id %s is down", applicationId));
		}
		return details;
	}

	private static Map<String, Object> taskDetails(Set<TaskMetadata> taskMetadata) {
		final Map<String, Object> details = new HashMap<>();
		for (TaskMetadata metadata : taskMetadata) {
			details.put("taskId", metadata.taskId());
			details.put("partitions",
					metadata.topicPartitions().stream().map(
							p -> "partition=" + p.partition() + ", topic=" + p.topic())
							.collect(Collectors.toList()));
		}
		return details;
	}

}
