/*
 * Copyright 2019-2021 the original author or authors.
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

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Health indicator for Kafka Streams.
 *
 * @author Arnaud Jardiné
 * @author Soby Chacko
 */
public class KafkaStreamsBinderHealthIndicator extends AbstractHealthIndicator implements DisposableBean {

	/**
	 * Static initialization for detecting whether the application is using Kafka client 2.5 vs lower versions.
	 */
	private static ClassLoader CLASS_LOADER = KafkaStreamsBinderHealthIndicator.class.getClassLoader();
	private static boolean isKafkaStreams25 = true;
	private static Method methodForIsRunning;

	static {
		try {
			Class<?> KAFKA_STREAMS_STATE_CLASS = CLASS_LOADER.loadClass("org.apache.kafka.streams.KafkaStreams$State");

			Method[] declaredMethods = KAFKA_STREAMS_STATE_CLASS.getDeclaredMethods();
			for (Method m : declaredMethods) {
				if (m.getName().equals("isRunning")) {
					isKafkaStreams25 = false;
					methodForIsRunning = m;
				}
			}
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("KafkaStreams$State class not found", e);
		}
	}

	private final KafkaStreamsRegistry kafkaStreamsRegistry;

	private final KafkaStreamsBinderConfigurationProperties configurationProperties;

	private final Map<String, Object> adminClientProperties;

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	private AdminClient adminClient;

	private final Lock lock = new ReentrantLock();

	KafkaStreamsBinderHealthIndicator(KafkaStreamsRegistry kafkaStreamsRegistry,
									  KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties,
									  KafkaProperties kafkaProperties,
									  KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue) {
		super("Kafka-streams health check failed");
		kafkaProperties.buildAdminProperties();
		this.configurationProperties = kafkaStreamsBinderConfigurationProperties;
		this.adminClientProperties = kafkaProperties.buildAdminProperties();
		KafkaTopicProvisioner.normalalizeBootPropsWithBinder(this.adminClientProperties, kafkaProperties,
				kafkaStreamsBinderConfigurationProperties);
		this.kafkaStreamsRegistry = kafkaStreamsRegistry;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
	}

	@Override
	protected void doHealthCheck(Health.Builder builder) throws Exception {
		try {
			this.lock.lock();
			if (this.adminClient == null) {
				this.adminClient = AdminClient.create(this.adminClientProperties);
			}

			final ListTopicsResult listTopicsResult = this.adminClient.listTopics();
			listTopicsResult.listings().get(this.configurationProperties.getHealthTimeout(), TimeUnit.SECONDS);

			if (this.kafkaStreamsBindingInformationCatalogue.getStreamsBuilderFactoryBeans().isEmpty()) {
				builder.withDetail("No Kafka Streams bindings have been established", "Kafka Streams binder did not detect any processors");
				builder.status(Status.UNKNOWN);
			}
			else {
				boolean up = true;
				for (KafkaStreams kStream : kafkaStreamsRegistry.getKafkaStreams()) {
					if (isKafkaStreams25) {
						up &= kStream.state().isRunningOrRebalancing();
					}
					else {
						// if Kafka client version is lower than 2.5, then call the method reflectively.
						final boolean isRunningInvokedResult = (boolean) methodForIsRunning.invoke(kStream.state());
						up &= isRunningInvokedResult;
					}
					builder.withDetails(buildDetails(kStream));
				}
				builder.status(up ? Status.UP : Status.DOWN);
			}
		}
		catch (Exception e) {
			builder.withDetail("No topic information available", "Kafka broker is not reachable");
			builder.status(Status.DOWN);
			builder.withException(e);
		}
		finally {
			this.lock.unlock();
		}
	}

	private Map<String, Object> buildDetails(KafkaStreams kafkaStreams) throws Exception {
		final Map<String, Object> details = new HashMap<>();
		final Map<String, Object> perAppdIdDetails = new HashMap<>();

		boolean isRunningResult;
		if (isKafkaStreams25) {
			isRunningResult = kafkaStreams.state().isRunningOrRebalancing();
		}
		else {
			// if Kafka client version is lower than 2.5, then call the method reflectively.
			isRunningResult = (boolean) methodForIsRunning.invoke(kafkaStreams.state());
		}

		if (isRunningResult) {
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
			if (details.containsKey("partitions")) {
				@SuppressWarnings("unchecked")
				List<String> partitionsInfo = (List<String>) details.get("partitions");
				partitionsInfo.addAll(addPartitionsInfo(metadata));
			}
			else {
				details.put("partitions",
						addPartitionsInfo(metadata));
			}
		}
		return details;
	}

	private static List<String> addPartitionsInfo(TaskMetadata metadata) {
		return metadata.topicPartitions().stream().map(
				p -> "partition=" + p.partition() + ", topic=" + p.topic())
				.collect(Collectors.toList());
	}

	@Override
	public void destroy() throws Exception {
		if (adminClient != null) {
			adminClient.close(Duration.ofSeconds(0));
		}
	}
}
