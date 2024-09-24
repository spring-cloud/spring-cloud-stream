/*
 * Copyright 2018-2024 the original author or authors.
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

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaTopicProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Aldo Sinanaj
 * @author Soby Chacko
 * @since 2.0
 *
 */
@SpringBootTest(classes = { KafkaBinderConfiguration.class,
		BindingServiceConfiguration.class })
@TestPropertySource(properties = {
		"spring.cloud.stream.kafka.bindings.input.consumer.topic.replication-factor=2",
		"spring.cloud.stream.kafka.bindings.input.consumer.topic.replicas-assignments.0=0,1",
		"spring.cloud.stream.kafka.bindings.input.consumer.topic.properties.message.format.version=0.9.0.0",
		"spring.cloud.stream.kafka.bindings.secondInput.consumer.topic.replication-factor=3",
		"spring.cloud.stream.kafka.bindings.secondInput.consumer.topic.replicas-assignments.0=0,1",
		"spring.cloud.stream.kafka.bindings.secondInput.consumer.topic.properties.message.format.version=0.9.1.0",
		"spring.cloud.stream.kafka.bindings.output.producer.topic.replication-factor=2",
		"spring.cloud.stream.kafka.bindings.output.producer.topic.replicas-assignments.0=0,1",
		"spring.cloud.stream.kafka.bindings.output.producer.topic.properties.message.format.version=0.9.0.0",
		"spring.main.allow-bean-definition-overriding=true" })
@EnableIntegration
class AdminConfigTests {

	@Autowired
	private KafkaMessageChannelBinder binder;

	@Test
	void consumerTopicProperties() {
		final KafkaConsumerProperties consumerProperties = this.binder
				.getExtendedConsumerProperties("secondInput");
		final KafkaTopicProperties kafkaTopicProperties = consumerProperties.getTopic();

		assertThat(kafkaTopicProperties.getReplicationFactor()).isEqualTo((short) 3);
		assertThat(kafkaTopicProperties.getReplicasAssignments().get(0))
				.isEqualTo(Arrays.asList(0, 1));
		assertThat(kafkaTopicProperties.getProperties().get("message.format.version"))
				.isEqualTo("0.9.1.0");
	}

	@Test
	void producerTopicProperties() {
		final KafkaProducerProperties producerProperties = this.binder
				.getExtendedProducerProperties("output");
		final KafkaTopicProperties kafkaTopicProperties = producerProperties.getTopic();

		assertThat(kafkaTopicProperties.getReplicationFactor()).isEqualTo((short) 2);
		assertThat(kafkaTopicProperties.getReplicasAssignments().get(0))
				.isEqualTo(Arrays.asList(0, 1));
		assertThat(kafkaTopicProperties.getProperties().get("message.format.version"))
				.isEqualTo("0.9.0.0");
	}

}
