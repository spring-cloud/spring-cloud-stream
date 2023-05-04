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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.pulsar.autoconfigure.ProducerConfigProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PulsarBinderUtils}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarBinderUtilsTests {

	@Nested
	class SubscriptionNameTests {

		@Test
		void respectsValueWhenSetAsProperty() {
			var consumerDestination = mock(ConsumerDestination.class);
			var pulsarConsumerProperties = mock(PulsarConsumerProperties.class);
			when(pulsarConsumerProperties.getSubscriptionName()).thenReturn("my-sub");
			assertThat(PulsarBinderUtils.subscriptionName(pulsarConsumerProperties, consumerDestination))
					.isEqualTo("my-sub");
		}

		@Test
		void generatesValueWhenNotSetAsProperty() {
			var consumerDestination = mock(ConsumerDestination.class);
			var pulsarConsumerProperties = mock(PulsarConsumerProperties.class);
			when(pulsarConsumerProperties.getSubscriptionName()).thenReturn(null);
			when(consumerDestination.getName()).thenReturn("my-topic");
			assertThat(PulsarBinderUtils.subscriptionName(pulsarConsumerProperties, consumerDestination))
					.startsWith("my-topic-anon-subscription-");
		}

	}

	@Nested
	class MergedPropertiesTests {

		@ParameterizedTest(name = "{0}")
		@MethodSource("mergePropertiesTestProvider")
		void mergePropertiesTest(String testName, Map<String, Object> baseProps, Map<String, Object> binderProps,
				Map<String, Object> bindingProps, Map<String, Object> expectedMergedProps) {
			assertThat(PulsarBinderUtils.mergePropertiesWithPrecedence(baseProps, binderProps, bindingProps))
					.containsExactlyInAnyOrderEntriesOf(expectedMergedProps);
		}

		// @formatter:off
		static Stream<Arguments> mergePropertiesTestProvider() {
			return Stream.of(
					arguments("binderLevelContainsSamePropAsBaseWithDiffValue",
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-binder"),
							Collections.emptyMap(),
							Map.of("foo", "foo-binder")),
					arguments("binderLevelContainsNewPropNotInBase",
							Collections.emptyMap(),
							Map.of("foo", "foo-binder"),
							Collections.emptyMap(),
							Map.of("foo", "foo-binder")),
					arguments("binderLevelContainsSamePropAsBaseWithSameValue",
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Collections.emptyMap()),
					arguments("bindingLevelContainsSamePropAsBaseWithDiffValue",
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Map.of("foo", "foo-binding"),
							Map.of("foo", "foo-binding")),
					arguments("bindingLevelContainsNewPropNotInBase",
							Collections.emptyMap(),
							Map.of("foo", "foo-binding"),
							Collections.emptyMap(),
							Map.of("foo", "foo-binding")),
					arguments("bindingLevelContainsSamePropAsBaseWithSameValue",
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Map.of("foo", "foo-base"),
							Collections.emptyMap()),
					arguments("bindingOverridesBinder",
							Map.of("bar", "bar-base"),
							Map.of("foo", "foo-binder"),
							Map.of("foo", "foo-binding"),
							Map.of("foo", "foo-binding")),
					arguments("binderOverridesBaseAndBindingOverridesBinder",
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-binder"),
							Map.of("foo", "foo-binding"),
							Map.of("foo", "foo-binding")),
					arguments("onlyBaseProps",
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Collections.emptyMap(),
							Collections.emptyMap()));
		}
		// @formatter:on

	}

	@Nested
	class ConvertedPropertiesTests {

		private final ProducerConfigProperties properties = new ProducerConfigProperties();

		private void bind(Map<String, String> map) {
			ConfigurationPropertySource source = new MapConfigurationPropertySource(map);
			new Binder(source).bind("spring.pulsar.producer", Bindable.ofInstance(this.properties));
		}

		@Test
		void producerPropertiesToMap() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.producer.topic-name", "my-topic");
			props.put("spring.pulsar.producer.producer-name", "my-producer");
			props.put("spring.pulsar.producer.send-timeout", "2s");
			props.put("spring.pulsar.producer.block-if-queue-full", "true");
			props.put("spring.pulsar.producer.max-pending-messages", "3");
			props.put("spring.pulsar.producer.max-pending-messages-across-partitions", "4");
			props.put("spring.pulsar.producer.message-routing-mode", "custompartition");
			props.put("spring.pulsar.producer.hashing-scheme", "murmur3_32hash");
			props.put("spring.pulsar.producer.crypto-failure-action", "send");
			props.put("spring.pulsar.producer.batching-max-publish-delay", "5s");
			props.put("spring.pulsar.producer.batching-partition-switch-frequency-by-publish-delay", "6");
			props.put("spring.pulsar.producer.batching-max-messages", "7");
			props.put("spring.pulsar.producer.batching-max-bytes", "8");
			props.put("spring.pulsar.producer.batching-enabled", "false");
			props.put("spring.pulsar.producer.chunking-enabled", "true");
			props.put("spring.pulsar.producer.encryption-keys[0]", "my-key");
			props.put("spring.pulsar.producer.compression-type", "lz4");
			props.put("spring.pulsar.producer.initial-sequence-id", "9");
			props.put("spring.pulsar.producer.producer-access-mode", "exclusive");
			props.put("spring.pulsar.producer.lazy-start=partitioned-producers", "true");
			props.put("spring.pulsar.producer.properties[my-prop]", "my-prop-value");

			bind(props);
			Map<String, Object> producerProps = PulsarBinderUtils.convertProducerPropertiesToMap(properties);

			// Verify that the props can be loaded in a ProducerBuilder
			assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(producerProps,
					new ProducerConfigurationData(), ProducerConfigurationData.class));

			assertThat(producerProps).containsEntry("topicName", "my-topic")
					.containsEntry("producerName", "my-producer").containsEntry("sendTimeoutMs", 2_000)
					.containsEntry("blockIfQueueFull", true).containsEntry("maxPendingMessages", 3)
					.containsEntry("maxPendingMessagesAcrossPartitions", 4)
					.containsEntry("messageRoutingMode", MessageRoutingMode.CustomPartition)
					.containsEntry("hashingScheme", HashingScheme.Murmur3_32Hash)
					.containsEntry("cryptoFailureAction", ProducerCryptoFailureAction.SEND)
					.containsEntry("batchingMaxPublishDelayMicros", 5_000_000L)
					.containsEntry("batchingPartitionSwitchFrequencyByPublishDelay", 6)
					.containsEntry("batchingMaxMessages", 7).containsEntry("batchingMaxBytes", 8)
					.containsEntry("batchingEnabled", false).containsEntry("chunkingEnabled", true)
					.hasEntrySatisfying("encryptionKeys",
							keys -> assertThat(keys).asInstanceOf(InstanceOfAssertFactories.collection(String.class))
									.containsExactly("my-key"))
					.containsEntry("compressionType", CompressionType.LZ4).containsEntry("initialSequenceId", 9L)
					.containsEntry("accessMode", ProducerAccessMode.Exclusive)
					.containsEntry("lazyStartPartitionedProducers", true).hasEntrySatisfying("properties",
							properties -> assertThat(properties)
									.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
									.containsEntry("my-prop", "my-prop-value"));
		}

	}

}
