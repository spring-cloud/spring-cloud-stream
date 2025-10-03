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

package org.springframework.cloud.stream.binder.pulsar;

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarExtendedBindingProperties;
import org.springframework.pulsar.listener.PulsarContainerProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Tests for {@link PulsarExtendedBindingProperties}.
 *
 * @author Chris Bono
 */
class PulsarExtendedBindingPropertiesTests {

	@Test
	void producerProperties() {
		// Only spot check a few values (ProducerConfigPropertiesTests does the heavy
		// lifting)
		Map<String, String> inputProps = new HashMap<>();
		inputProps.put("spring.cloud.stream.pulsar.bindings.my-foo.producer.topic-name", "my-topic");
		inputProps.put("spring.cloud.stream.pulsar.bindings.my-foo.producer.send-timeout", "2s");
		inputProps.put("spring.cloud.stream.pulsar.bindings.my-foo.producer.max-pending-messages", "3");
		inputProps.put("spring.cloud.stream.pulsar.bindings.my-foo.producer.access-mode", "exclusive");
		var bindingConfigProps = bindInputPropsToBindingConfigProps(inputProps);
		assertThat(bindingConfigProps.getBindings()).containsOnlyKeys("my-foo");
		var producerProps = bindingConfigProps.getExtendedProducerProperties("my-foo").toAllProducerPropertiesMap();

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
		inputProps.put("spring.cloud.stream.pulsar.bindings.my-foo.consumer.topics[0]", "my-topic");
		inputProps.put("spring.cloud.stream.pulsar.bindings.my-foo.consumer.subscription.mode", "nondurable");
		inputProps.put("spring.cloud.stream.pulsar.bindings.my-foo.consumer.receiver-queue-size", "1");
		var bindingConfigProps = bindInputPropsToBindingConfigProps(inputProps);
		assertThat(bindingConfigProps.getBindings()).containsOnlyKeys("my-foo");
		var consumerProps = bindingConfigProps.getExtendedConsumerProperties("my-foo").toAllConsumerPropertiesMap();

		// Verify that the props can be loaded in a ConsumerBuilder
		assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(consumerProps,
				new ConsumerConfigurationData<>(), ConsumerConfigurationData.class));
		assertThat(consumerProps)
				.hasEntrySatisfying("topicNames",
						topics -> assertThat(topics).asInstanceOf(InstanceOfAssertFactories.collection(String.class))
								.containsExactly("my-topic"))
				.containsEntry("subscriptionMode", SubscriptionMode.NonDurable).containsEntry("receiverQueueSize", 1);
	}

	@Test
	void extendedBindingsArePropagatedToContainerProperties() {
		Map<String, String> inputProps = new HashMap<>();
		inputProps.put("spring.cloud.stream.pulsar.bindings.my-foo.consumer.subscription.name", "my-foo-sbscription");
		inputProps.put("spring.cloud.stream.pulsar.bindings.my-foo.consumer.subscription.type", "Shared");
		var bindingConfigProps = bindInputPropsToBindingConfigProps(inputProps);
		var consumerProps = bindingConfigProps.getExtendedConsumerProperties("my-foo").toAllConsumerPropertiesMap();

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.getPulsarConsumerProperties().putAll(consumerProps);
		assertThat(pulsarContainerProperties.getSubscriptionName()).isNull();
		assertThat(pulsarContainerProperties.getSubscriptionType()).isNull();
		pulsarContainerProperties.updateContainerProperties();
		assertThat(pulsarContainerProperties.getSubscriptionName()).isEqualTo("my-foo-sbscription");
		assertThat(pulsarContainerProperties.getSubscriptionType()).isEqualTo(SubscriptionType.Shared);
	}

	private PulsarExtendedBindingProperties bindInputPropsToBindingConfigProps(Map<String, String> inputProps) {
		return new Binder(new MapConfigurationPropertySource(inputProps))
				.bind("spring.cloud.stream.pulsar", Bindable.ofInstance(new PulsarExtendedBindingProperties())).get();
	}

}
