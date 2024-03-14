/*
 * Copyright 2023-2024 the original author or authors.
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

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.cloud.stream.binder.pulsar.properties.ConsumerConfigProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.entry;

/**
 * Unit tests for {@link ConsumerConfigProperties}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
class ConsumerConfigPropertiesTests {

	@Test
	void allBasePropsCanBeExtractedToMap() {
		var inputProps = basePropsInputMap();
		var consumerConfigProps = bindInputPropsToConsumerConfigProps(inputProps);
		var outputProps = consumerConfigProps.toBaseConsumerPropertiesMap();
		verifyOutputPropsCanBeLoadedInConsumerBuilder(outputProps);
		verifyBasePropsInOutputMap(outputProps);
	}

	@Test
	void nullBasePropsSkippedWhenExtractedToMap() {
		var inputProps = Map.of("spring.pulsar.consumer.name", "my-consumer");
		var consumerConfigProps = bindInputPropsToConsumerConfigProps(inputProps);
		var outputProps = consumerConfigProps.toBaseConsumerPropertiesMap();
		assertThat(outputProps).contains(entry("consumerName", "my-consumer"));
		assertThat(outputProps).doesNotContainKey("deadLetterPolicy");
	}

	private Map<String, String> basePropsInputMap() {
		Map<String, String> inputProps = new HashMap<>();
		inputProps.put("spring.pulsar.consumer.name", "my-consumer");
		inputProps.put("spring.pulsar.consumer.priority-level", "8");
		inputProps.put("spring.pulsar.consumer.read-compacted", "true");
		inputProps.put("spring.pulsar.consumer.retry-enable", "true");
		inputProps.put("spring.pulsar.consumer.topics[0]", "my-topic");
		inputProps.put("spring.pulsar.consumer.topics-pattern", "my-pattern");
		// DeadLetterPolicy
		inputProps.put("spring.pulsar.consumer.dead-letter-policy.max-redeliver-count", "4");
		inputProps.put("spring.pulsar.consumer.dead-letter-policy.retry-letter-topic", "my-retry-topic");
		inputProps.put("spring.pulsar.consumer.dead-letter-policy.dead-letter-topic", "my-dlt-topic");
		inputProps.put("spring.pulsar.consumer.dead-letter-policy.initial-subscription-name",
				"my-initial-subscription");
		// Subscription
		inputProps.put("spring.pulsar.consumer.subscription.name", "my-subscription");
		inputProps.put("spring.pulsar.consumer.subscription.type", "exclusive");
		inputProps.put("spring.pulsar.consumer.subscription.mode", "non-durable");
		inputProps.put("spring.pulsar.consumer.subscription.initial-position", "earliest");
		inputProps.put("spring.pulsar.consumer.subscription.topics-mode", "all-topics");
		return inputProps;
	}

	private void verifyBasePropsInOutputMap(Map<String, Object> outputProps) {
		assertThat(outputProps)
				.containsEntry("consumerName", "my-consumer")
				.containsEntry("priorityLevel", 8)
				.containsEntry("readCompacted", true)
				.containsEntry("retryEnable", true)
				.hasEntrySatisfying("topicNames", topics ->
						assertThat(topics).asInstanceOf(InstanceOfAssertFactories.collection(String.class))
								.containsExactly("my-topic"))
				.hasEntrySatisfying("topicsPattern", p -> assertThat(p.toString()).isEqualTo("my-pattern"))
				.containsEntry("subscriptionName", "my-subscription")
				.containsEntry("subscriptionType", SubscriptionType.Exclusive)
				.containsEntry("subscriptionMode", SubscriptionMode.NonDurable)
				.containsEntry("subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
				.containsEntry("regexSubscriptionMode", RegexSubscriptionMode.AllTopics)
				.hasEntrySatisfying("deadLetterPolicy", dlp -> {
					DeadLetterPolicy deadLetterPolicy = (DeadLetterPolicy) dlp;
					assertThat(deadLetterPolicy.getMaxRedeliverCount()).isEqualTo(4);
					assertThat(deadLetterPolicy.getRetryLetterTopic()).isEqualTo("my-retry-topic");
					assertThat(deadLetterPolicy.getDeadLetterTopic()).isEqualTo("my-dlt-topic");
					assertThat(deadLetterPolicy.getInitialSubscriptionName()).isEqualTo("my-initial-subscription");
				});
	}

	@Test
	void extendedPropsCanBeExtractedToMap() {
		var inputProps = basePropsInputMap();
		inputProps.putAll(extendedPropsInputMap());
		var consumerConfigProps = bindInputPropsToConsumerConfigProps(inputProps);
		var outputProps = consumerConfigProps.toExtendedConsumerPropertiesMap();
		verifyOutputPropsCanBeLoadedInConsumerBuilder(outputProps);
		verifyExtendedPropsInOutputMap(outputProps);
	}

	@Test
	void allPropsCanBeExtractedToMap() {
		var inputProps = basePropsInputMap();
		inputProps.putAll(extendedPropsInputMap());
		var consumerConfigProps = bindInputPropsToConsumerConfigProps(inputProps);
		var outputProps = consumerConfigProps.toAllConsumerPropertiesMap();
		verifyOutputPropsCanBeLoadedInConsumerBuilder(outputProps);
		verifyBasePropsInOutputMap(outputProps);
		verifyExtendedPropsInOutputMap(outputProps);
	}

	private Map<String, String> extendedPropsInputMap() {
		Map<String, String> inputProps = new HashMap<>();
		inputProps.put("spring.pulsar.consumer.auto-update-partitions", "true");
		inputProps.put("spring.pulsar.consumer.auto-update-partitions-interval", "10s");
		inputProps.put("spring.pulsar.consumer.crypto-failure-action", "discard");
		inputProps.put("spring.pulsar.consumer.max-total-receiver-queue-size-across-partitions", "5");
		inputProps.put("spring.pulsar.consumer.pattern-auto-discovery-period", "9");
		inputProps.put("spring.pulsar.consumer.pool-messages", "true");
		inputProps.put("spring.pulsar.consumer.properties[my-prop]", "my-prop-value");
		inputProps.put("spring.pulsar.consumer.receiver-queue-size", "1");
		inputProps.put("spring.pulsar.consumer.reset-include-head", "true");
		inputProps.put("spring.pulsar.consumer.start-paused", "true");
		// Acknowledgment
		inputProps.put("spring.pulsar.consumer.ack.group-time", "2s");
		inputProps.put("spring.pulsar.consumer.ack.redelivery-delay", "3s");
		inputProps.put("spring.pulsar.consumer.ack.timeout", "6s");
		inputProps.put("spring.pulsar.consumer.ack.timeout-tick-duration", "7s");
		inputProps.put("spring.pulsar.consumer.ack.batch-index-enabled", "true");
		inputProps.put("spring.pulsar.consumer.ack.receipt-enabled", "true");
		// Chunk
		inputProps.put("spring.pulsar.consumer.chunk.expire-time-incomplete", "12s");
		inputProps.put("spring.pulsar.consumer.chunk.auto-ack-oldest-on-queue-full", "false");
		inputProps.put("spring.pulsar.consumer.chunk.max-pending-messages", "11");
		// Subscription
		inputProps.put("spring.pulsar.consumer.subscription.properties[my-sub-prop]", "my-sub-prop-value");
		inputProps.put("spring.pulsar.consumer.subscription.replicate-state", "true");
		return inputProps;
	}

	private void verifyExtendedPropsInOutputMap(Map<String, Object> outputProps) {
		assertThat(outputProps).containsEntry("autoUpdatePartitions", true)
				.containsEntry("autoUpdatePartitionsIntervalSeconds", 10L)
				.containsEntry("cryptoFailureAction", ConsumerCryptoFailureAction.DISCARD)
				.containsEntry("maxTotalReceiverQueueSizeAcrossPartitions", 5)
				.containsEntry("patternAutoDiscoveryPeriod", 9)
				.containsEntry("poolMessages", true)
				.hasEntrySatisfying("properties",
						properties -> assertThat(properties)
								.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
								.containsEntry("my-prop", "my-prop-value"))
				.containsEntry("receiverQueueSize", 1)
				.containsEntry("resetIncludeHead", true)
				.containsEntry("startPaused", true)
				.containsEntry("acknowledgementsGroupTimeMicros", 2_000_000L)
				.containsEntry("negativeAckRedeliveryDelayMicros", 3_000_000L)
				.containsEntry("ackTimeoutMillis", 6_000L)
				.containsEntry("tickDurationMillis", 7_000L)
				.containsEntry("batchIndexAckEnabled", true)
				.containsEntry("ackReceiptEnabled", true)
				.containsEntry("expireTimeOfIncompleteChunkedMessageMillis", 12_000L)
				.containsEntry("autoAckOldestChunkedMessageOnQueueFull", false)
				.containsEntry("maxPendingChunkedMessage", 11)
				.hasEntrySatisfying("subscriptionProperties",
						properties -> assertThat(properties)
								.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
								.containsEntry("my-sub-prop", "my-sub-prop-value"))
				.containsEntry("replicateSubscriptionState", true);
	}

	private void verifyOutputPropsCanBeLoadedInConsumerBuilder(Map<String, Object> outputProps) {
		assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(outputProps,
				new ConsumerConfigurationData<>(), ConsumerConfigurationData.class));
	}

	private ConsumerConfigProperties bindInputPropsToConsumerConfigProps(Map<String, String> inputProps) {
		return new Binder(new MapConfigurationPropertySource(inputProps))
				.bind("spring.pulsar.consumer", Bindable.ofInstance(new ConsumerConfigProperties())).get();
	}

}
