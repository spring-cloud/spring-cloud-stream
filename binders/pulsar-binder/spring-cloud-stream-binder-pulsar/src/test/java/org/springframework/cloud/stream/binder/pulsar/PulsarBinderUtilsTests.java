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
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
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
import org.springframework.pulsar.autoconfigure.ConsumerConfigProperties;
import org.springframework.pulsar.autoconfigure.ProducerConfigProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
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

	@Nested
	class ConvertedConsumerPropertiesTests {

		private final ConsumerConfigProperties properties = new ConsumerConfigProperties();

		private void bind(Map<String, String> map) {
			ConfigurationPropertySource source = new MapConfigurationPropertySource(map);
			new Binder(source).bind("spring.pulsar.consumer", Bindable.ofInstance(this.properties));
		}

		@Test
		void consumerPropertiesToMap() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.consumer.topics[0]", "my-topic");
			props.put("spring.pulsar.consumer.topics-pattern", "my-pattern");
			props.put("spring.pulsar.consumer.subscription-name", "my-subscription");
			props.put("spring.pulsar.consumer.subscription-type", "shared");
			props.put("spring.pulsar.consumer.subscription-properties[my-sub-prop]", "my-sub-prop-value");
			props.put("spring.pulsar.consumer.subscription-mode", "nondurable");
			props.put("spring.pulsar.consumer.receiver-queue-size", "1");
			props.put("spring.pulsar.consumer.acknowledgements-group-time", "2s");
			props.put("spring.pulsar.consumer.negative-ack-redelivery-delay", "3s");
			props.put("spring.pulsar.consumer.max-total-receiver-queue-size-across-partitions", "5");
			props.put("spring.pulsar.consumer.consumer-name", "my-consumer");
			props.put("spring.pulsar.consumer.ack-timeout", "6s");
			props.put("spring.pulsar.consumer.tick-duration", "7s");
			props.put("spring.pulsar.consumer.priority-level", "8");
			props.put("spring.pulsar.consumer.crypto-failure-action", "discard");
			props.put("spring.pulsar.consumer.properties[my-prop]", "my-prop-value");
			props.put("spring.pulsar.consumer.read-compacted", "true");
			props.put("spring.pulsar.consumer.subscription-initial-position", "earliest");
			props.put("spring.pulsar.consumer.pattern-auto-discovery-period", "9");
			props.put("spring.pulsar.consumer.regex-subscription-mode", "all-topics");
			props.put("spring.pulsar.consumer.dead-letter-policy.max-redeliver-count", "4");
			props.put("spring.pulsar.consumer.dead-letter-policy.retry-letter-topic", "my-retry-topic");
			props.put("spring.pulsar.consumer.dead-letter-policy.dead-letter-topic", "my-dlt-topic");
			props.put("spring.pulsar.consumer.dead-letter-policy.initial-subscription-name", "my-initial-subscription");
			props.put("spring.pulsar.consumer.retry-enable", "true");
			props.put("spring.pulsar.consumer.auto-update-partitions", "false");
			props.put("spring.pulsar.consumer.auto-update-partitions-interval", "10s");
			props.put("spring.pulsar.consumer.replicate-subscription-state", "true");
			props.put("spring.pulsar.consumer.reset-include-head", "true");
			props.put("spring.pulsar.consumer.batch-index-ack-enabled", "true");
			props.put("spring.pulsar.consumer.ack-receipt-enabled", "true");
			props.put("spring.pulsar.consumer.pool-messages", "true");
			props.put("spring.pulsar.consumer.start-paused", "true");
			props.put("spring.pulsar.consumer.auto-ack-oldest-chunked-message-on-queue-full", "false");
			props.put("spring.pulsar.consumer.max-pending-chunked-message", "11");
			props.put("spring.pulsar.consumer.expire-time-of-incomplete-chunked-message", "12s");

			bind(props);
			Map<String, Object> consumerProps = PulsarBinderUtils.convertConsumerPropertiesToMap(properties);

			// Verify that the props can be loaded in a ConsumerBuilder
			assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(consumerProps,
					new ConsumerConfigurationData<>(), ConsumerConfigurationData.class));

			assertThat(consumerProps)
					.hasEntrySatisfying("topicNames",
							topics -> assertThat(topics)
									.asInstanceOf(InstanceOfAssertFactories.collection(String.class))
									.containsExactly("my-topic"))
					.hasEntrySatisfying("topicsPattern", p -> assertThat(p.toString()).isEqualTo("my-pattern"))
					.containsEntry("subscriptionName", "my-subscription")
					.containsEntry("subscriptionType", SubscriptionType.Shared)
					.hasEntrySatisfying("subscriptionProperties",
							properties -> assertThat(properties)
									.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
									.containsEntry("my-sub-prop", "my-sub-prop-value"))
					.containsEntry("subscriptionMode", SubscriptionMode.NonDurable)
					.containsEntry("receiverQueueSize", 1).containsEntry("acknowledgementsGroupTimeMicros", 2_000_000L)
					.containsEntry("negativeAckRedeliveryDelayMicros", 3_000_000L)
					.containsEntry("maxTotalReceiverQueueSizeAcrossPartitions", 5)
					.containsEntry("consumerName", "my-consumer").containsEntry("ackTimeoutMillis", 6_000L)
					.containsEntry("tickDurationMillis", 7_000L).containsEntry("priorityLevel", 8)
					.containsEntry("cryptoFailureAction", ConsumerCryptoFailureAction.DISCARD)
					.hasEntrySatisfying("properties",
							properties -> assertThat(properties)
									.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
									.containsEntry("my-prop", "my-prop-value"))
					.containsEntry("readCompacted", true)
					.containsEntry("subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
					.containsEntry("patternAutoDiscoveryPeriod", 9)
					.containsEntry("regexSubscriptionMode", RegexSubscriptionMode.AllTopics)
					.hasEntrySatisfying("deadLetterPolicy", dlp -> {
						DeadLetterPolicy deadLetterPolicy = (DeadLetterPolicy) dlp;
						assertThat(deadLetterPolicy.getMaxRedeliverCount()).isEqualTo(4);
						assertThat(deadLetterPolicy.getRetryLetterTopic()).isEqualTo("my-retry-topic");
						assertThat(deadLetterPolicy.getDeadLetterTopic()).isEqualTo("my-dlt-topic");
						assertThat(deadLetterPolicy.getInitialSubscriptionName()).isEqualTo("my-initial-subscription");
					}).containsEntry("retryEnable", true).containsEntry("autoUpdatePartitions", false)
					.containsEntry("autoUpdatePartitionsIntervalSeconds", 10L)
					.containsEntry("replicateSubscriptionState", true).containsEntry("resetIncludeHead", true)
					.containsEntry("batchIndexAckEnabled", true).containsEntry("ackReceiptEnabled", true)
					.containsEntry("poolMessages", true).containsEntry("startPaused", true)
					.containsEntry("autoAckOldestChunkedMessageOnQueueFull", false)
					.containsEntry("maxPendingChunkedMessage", 11)
					.containsEntry("expireTimeOfIncompleteChunkedMessageMillis", 12_000L);
		}

	}

}
