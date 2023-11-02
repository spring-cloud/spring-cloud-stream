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

package org.springframework.cloud.stream.binder.pulsar;

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarBinderConfigurationProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Tests for {@link PulsarBinderConfigurationProperties}.
 *
 * @author Chris Bono
 */
class PulsarBinderConfigurationPropertiesTests {

	@Test
	void partitionCountProperty() {
		assertThat(new PulsarBinderConfigurationProperties().getPartitionCount()).isNull();
		var binderConfigProps = bindInputPropsToBinderConfigProps(
				Map.of("spring.cloud.stream.pulsar.binder.partition-count", "5150"));
		assertThat(binderConfigProps.getPartitionCount()).isEqualTo(5150);
	}

	@Test
	void producerProperties() {
		// Only spot check a few values (ProducerConfigPropertiesTests does the heavy
		// lifting)
		Map<String, String> inputProps = new HashMap<>();
		inputProps.put("spring.cloud.stream.pulsar.binder.producer.topic-name", "my-topic");
		inputProps.put("spring.cloud.stream.pulsar.binder.producer.send-timeout", "2s");
		inputProps.put("spring.cloud.stream.pulsar.binder.producer.max-pending-messages", "3");
		inputProps.put("spring.cloud.stream.pulsar.binder.producer.access-mode", "exclusive");
		var binderConfigProps = bindInputPropsToBinderConfigProps(inputProps);
		var producerProps = binderConfigProps.getProducer().toAllProducerPropertiesMap();

		// Verify that the props can be loaded in a ProducerBuilder
		assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(producerProps,
				new ProducerConfigurationData(), ProducerConfigurationData.class));
		assertThat(producerProps).containsEntry("topicName", "my-topic").containsEntry("sendTimeoutMs", 2_000)
				.containsEntry("maxPendingMessages", 3).containsEntry("accessMode", ProducerAccessMode.Exclusive);
	}

	@Test
	void consumerProperties() {
		// Only spot check a few values (ConsumerConfigPropertiesTests does the heavy
		// lifting)
		Map<String, String> inputProps = new HashMap<>();
		inputProps.put("spring.cloud.stream.pulsar.binder.consumer.topics[0]", "my-topic");
		inputProps.put("spring.cloud.stream.pulsar.binder.consumer.subscription.mode", "nondurable");
		inputProps.put("spring.cloud.stream.pulsar.binder.consumer.receiver-queue-size", "1");
		var binderConfigProps = bindInputPropsToBinderConfigProps(inputProps);
		var consumerProps = binderConfigProps.getConsumer().toAllConsumerPropertiesMap();

		// Verify that the props can be loaded in a ConsumerBuilder
		assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(consumerProps,
				new ConsumerConfigurationData<>(), ConsumerConfigurationData.class));
		assertThat(consumerProps)
				.hasEntrySatisfying("topicNames",
						topics -> assertThat(topics).asInstanceOf(InstanceOfAssertFactories.collection(String.class))
								.containsExactly("my-topic"))
				.containsEntry("subscriptionMode", SubscriptionMode.NonDurable).containsEntry("receiverQueueSize", 1);
	}

	private PulsarBinderConfigurationProperties bindInputPropsToBinderConfigProps(Map<String, String> inputProps) {
		return new Binder(new MapConfigurationPropertySource(inputProps)).bind("spring.cloud.stream.pulsar.binder",
				Bindable.ofInstance(new PulsarBinderConfigurationProperties())).get();
	}

}
