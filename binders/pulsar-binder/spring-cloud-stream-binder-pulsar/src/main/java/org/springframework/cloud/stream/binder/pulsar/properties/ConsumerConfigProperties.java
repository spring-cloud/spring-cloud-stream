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

package org.springframework.cloud.stream.binder.pulsar.properties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.MessageId;

import org.springframework.boot.autoconfigure.pulsar.PulsarProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.util.Assert;

/**
 * Configuration properties used to specify Pulsar consumers.
 *
 * @author Chris Bono
 */
public class ConsumerConfigProperties extends PulsarProperties.Consumer {

	private final Acknowledgement ack = new Acknowledgement();

	private final Chunking chunk = new Chunking();

	private final Subscription subscription = new Subscription();

	/**
	 * Number of messages that can be accumulated before the consumer calls "receive".
	 */
	private Integer receiverQueueSize = 1000;

	/**
	 * Maximum number of messages that a consumer can be pushed at once from a broker
	 * across all partitions.
	 */
	private Integer maxTotalReceiverQueueSizeAcrossPartitions = 50000;

	/**
	 * Action the consumer will take in case of decryption failure.
	 */
	private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

	/**
	 * Map of properties to add to the consumer.
	 */
	private SortedMap<String, String> properties = new TreeMap<>();

	/**
	 * Auto-discovery period for topics when topic pattern is used in minutes.
	 */
	private Integer patternAutoDiscoveryPeriod = 1;

	/**
	 * Whether the consumer auto-subscribes for partition increase. This is only for
	 * partitioned consumers.
	 */
	private Boolean autoUpdatePartitions = true;

	/**
	 * Interval of partitions discovery updates.
	 */
	private Duration autoUpdatePartitionsInterval = Duration.ofMinutes(1);

	/**
	 * Whether to include the given position of any reset operation like
	 * {@link org.apache.pulsar.client.api.Consumer#seek(long) or
	 * {@link ConsumerConfigProperties#seek(MessageId)}}.
	 */
	private Boolean resetIncludeHead = false;

	/**
	 * Whether pooling of messages and the underlying data buffers is enabled.
	 */
	private Boolean poolMessages = false;

	/**
	 * Whether to start the consumer in a paused state.
	 */
	private Boolean startPaused = false;

	public Acknowledgement getAck() {
		return this.ack;
	}

	public Chunking getChunk() {
		return this.chunk;
	}

	public Subscription getSubscription() {
		return this.subscription;
	}

	public Integer getReceiverQueueSize() {
		return this.receiverQueueSize;
	}

	public void setReceiverQueueSize(Integer receiverQueueSize) {
		this.receiverQueueSize = receiverQueueSize;
	}

	public Integer getMaxTotalReceiverQueueSizeAcrossPartitions() {
		return this.maxTotalReceiverQueueSizeAcrossPartitions;
	}

	public void setMaxTotalReceiverQueueSizeAcrossPartitions(Integer maxTotalReceiverQueueSizeAcrossPartitions) {
		this.maxTotalReceiverQueueSizeAcrossPartitions = maxTotalReceiverQueueSizeAcrossPartitions;
	}

	public ConsumerCryptoFailureAction getCryptoFailureAction() {
		return this.cryptoFailureAction;
	}

	public void setCryptoFailureAction(ConsumerCryptoFailureAction cryptoFailureAction) {
		this.cryptoFailureAction = cryptoFailureAction;
	}

	public SortedMap<String, String> getProperties() {
		return this.properties;
	}

	public void setProperties(SortedMap<String, String> properties) {
		this.properties = properties;
	}

	public Integer getPatternAutoDiscoveryPeriod() {
		return this.patternAutoDiscoveryPeriod;
	}

	public void setPatternAutoDiscoveryPeriod(Integer patternAutoDiscoveryPeriod) {
		this.patternAutoDiscoveryPeriod = patternAutoDiscoveryPeriod;
	}

	public Boolean getAutoUpdatePartitions() {
		return this.autoUpdatePartitions;
	}

	public void setAutoUpdatePartitions(Boolean autoUpdatePartitions) {
		this.autoUpdatePartitions = autoUpdatePartitions;
	}

	public Duration getAutoUpdatePartitionsInterval() {
		return this.autoUpdatePartitionsInterval;
	}

	public void setAutoUpdatePartitionsInterval(Duration autoUpdatePartitionsInterval) {
		this.autoUpdatePartitionsInterval = autoUpdatePartitionsInterval;
	}

	public Boolean getResetIncludeHead() {
		return this.resetIncludeHead;
	}

	public void setResetIncludeHead(Boolean resetIncludeHead) {
		this.resetIncludeHead = resetIncludeHead;
	}

	public Boolean getPoolMessages() {
		return this.poolMessages;
	}

	public void setPoolMessages(Boolean poolMessages) {
		this.poolMessages = poolMessages;
	}

	public Boolean getStartPaused() {
		return this.startPaused;
	}

	public void setStartPaused(Boolean startPaused) {
		this.startPaused = startPaused;
	}

	/**
	 * Gets a map representation of the base consumer properties (those defined in parent
	 * class).
	 * @return map of base consumer properties and associated values.
	 */
	public Map<String, Object> toBaseConsumerPropertiesMap() {
		var consumerProps = new ConsumerConfigProperties.Properties();
		var map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		map.from(this::getDeadLetterPolicy).as(this::toPulsarDeadLetterPolicy).to(consumerProps.in("deadLetterPolicy"));
		map.from(this::getName).to(consumerProps.in("consumerName"));
		map.from(this::getPriorityLevel).to(consumerProps.in("priorityLevel"));
		map.from(this::isReadCompacted).to(consumerProps.in("readCompacted"));
		map.from(this::isRetryEnable).to(consumerProps.in("retryEnable"));
		map.from(this::getTopics).to(consumerProps.in("topicNames"));
		map.from(this::getTopicsPattern).to(consumerProps.in("topicsPattern"));
		mapBaseSubscriptionProperties(this.getSubscription(), consumerProps, map);
		return consumerProps;
	}

	private org.apache.pulsar.client.api.DeadLetterPolicy toPulsarDeadLetterPolicy(DeadLetterPolicy policy) {
		Assert.state(policy.getMaxRedeliverCount() > 0,
				"Pulsar DeadLetterPolicy must have a positive 'max-redelivery-count' property value");
		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		org.apache.pulsar.client.api.DeadLetterPolicy.DeadLetterPolicyBuilder builder = org.apache.pulsar.client.api.DeadLetterPolicy
				.builder();
		map.from(policy::getMaxRedeliverCount).to(builder::maxRedeliverCount);
		map.from(policy::getRetryLetterTopic).to(builder::retryLetterTopic);
		map.from(policy::getDeadLetterTopic).to(builder::deadLetterTopic);
		map.from(policy::getInitialSubscriptionName).to(builder::initialSubscriptionName);
		return builder.build();
	}

	private void mapBaseSubscriptionProperties(PulsarProperties.Consumer.Subscription subscription, Properties consumerProps, PropertyMapper map) {
		map.from(subscription::getName).to(consumerProps.in("subscriptionName"));
		map.from(subscription::getType).to(consumerProps.in("subscriptionType"));
		map.from(subscription::getMode).to(consumerProps.in("subscriptionMode"));
		map.from(subscription::getInitialPosition).to(consumerProps.in("subscriptionInitialPosition"));
		map.from(subscription::getTopicsMode).to(consumerProps.in("regexSubscriptionMode"));
	}

	/**
	 * Gets a map representation of the extended consumer properties (those defined in
	 * this class).
	 * @return map of extended consumer properties and associated values.
	 */
	public Map<String, Object> toExtendedConsumerPropertiesMap() {
		var consumerProps = new ConsumerConfigProperties.Properties();
		var map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		map.from(this::getAutoUpdatePartitions).to(consumerProps.in("autoUpdatePartitions"));
		map.from(this::getAutoUpdatePartitionsInterval).as(Duration::toSeconds)
				.to(consumerProps.in("autoUpdatePartitionsIntervalSeconds"));
		map.from(this::getCryptoFailureAction).to(consumerProps.in("cryptoFailureAction"));
		map.from(this::getMaxTotalReceiverQueueSizeAcrossPartitions)
				.to(consumerProps.in("maxTotalReceiverQueueSizeAcrossPartitions"));
		map.from(this::getPatternAutoDiscoveryPeriod).to(consumerProps.in("patternAutoDiscoveryPeriod"));
		map.from(this::getPoolMessages).to(consumerProps.in("poolMessages"));
		map.from(this::getProperties).to(consumerProps.in("properties"));
		map.from(this::getReceiverQueueSize).to(consumerProps.in("receiverQueueSize"));
		map.from(this::getResetIncludeHead).to(consumerProps.in("resetIncludeHead"));
		map.from(this::getStartPaused).to(consumerProps.in("startPaused"));
		this.mapAcknowledgementProperties(this.getAck(), consumerProps, map);
		this.mapChunkProperties(this.getChunk(), consumerProps, map);
		this.mapExtendedSubscriptionProperties(this.getSubscription(), consumerProps, map);
		return consumerProps;
	}

	private void mapAcknowledgementProperties(Acknowledgement ack, Properties consumerProps, PropertyMapper map) {
		map.from(ack::getGroupTime).as(it -> it.toNanos() / 1000)
				.to(consumerProps.in("acknowledgementsGroupTimeMicros"));
		map.from(ack::getRedeliveryDelay).as(it -> it.toNanos() / 1000)
			.to(consumerProps.in("negativeAckRedeliveryDelayMicros"));
		map.from(ack::getTimeout).as(Duration::toMillis)
				.to(consumerProps.in("ackTimeoutMillis"));
		map.from(ack::getTimeoutTickDuration).as(Duration::toMillis)
				.to(consumerProps.in("tickDurationMillis"));
		map.from(ack::getBatchIndexEnabled).to(consumerProps.in("batchIndexAckEnabled"));
		map.from(ack::getReceiptEnabled).to(consumerProps.in("ackReceiptEnabled"));
	}

	private void mapChunkProperties(Chunking chunk, Properties consumerProps, PropertyMapper map) {
		map.from(chunk::getExpireTimeIncomplete).as(Duration::toMillis)
				.to(consumerProps.in("expireTimeOfIncompleteChunkedMessageMillis"));
		map.from(chunk::getAutoAckOldestOnQueueFull)
				.to(consumerProps.in("autoAckOldestChunkedMessageOnQueueFull"));
		map.from(chunk::getMaxPendingMessages).to(consumerProps.in("maxPendingChunkedMessage"));
	}

	private void mapExtendedSubscriptionProperties(Subscription subscription, Properties consumerProps, PropertyMapper map) {
		map.from(subscription::getProperties).to(consumerProps.in("subscriptionProperties"));
		map.from(subscription::getReplicateState).to(consumerProps.in("replicateSubscriptionState"));
	}

	/**
	 * Gets a map representation of base and extended consumer properties.
	 * @return map of base and extended consumer properties and associated values.
	 */
	public Map<String, Object> toAllConsumerPropertiesMap() {
		var consumerProps = this.toBaseConsumerPropertiesMap();
		consumerProps.putAll(this.toExtendedConsumerPropertiesMap());
		return consumerProps;
	}

	public static class Acknowledgement {

		/**
		 * Whether the batching index acknowledgment is enabled.
		 */
		private Boolean batchIndexEnabled = false;

		/**
		 * Time to group acknowledgements before sending them to the broker.
		 */
		private Duration groupTime = Duration.ofMillis(100);

		/**
		 * Whether an acknowledgement receipt is enabled.
		 */
		private Boolean receiptEnabled = false;

		/**
		 * Delay before re-delivering messages that have failed to be processed.
		 */
		private Duration redeliveryDelay = Duration.ofMinutes(1);

		/**
		 * Timeout for unacked messages to be redelivered.
		 */
		private Duration timeout = Duration.ZERO;

		/**
		 * Precision for the ack timeout messages tracker.
		 */
		private Duration timeoutTickDuration = Duration.ofSeconds(1);

		public Boolean getBatchIndexEnabled() {
			return this.batchIndexEnabled;
		}

		public void setBatchIndexEnabled(Boolean batchIndexEnabled) {
			this.batchIndexEnabled = batchIndexEnabled;
		}

		public Duration getGroupTime() {
			return this.groupTime;
		}

		public void setGroupTime(Duration groupTime) {
			this.groupTime = groupTime;
		}

		public Boolean getReceiptEnabled() {
			return this.receiptEnabled;
		}

		public void setReceiptEnabled(Boolean receiptEnabled) {
			this.receiptEnabled = receiptEnabled;
		}

		public Duration getRedeliveryDelay() {
			return this.redeliveryDelay;
		}

		public void setRedeliveryDelay(Duration redeliveryDelay) {
			this.redeliveryDelay = redeliveryDelay;
		}

		public Duration getTimeout() {
			return this.timeout;
		}

		public void setTimeout(Duration timeout) {
			this.timeout = timeout;
		}

		public Duration getTimeoutTickDuration() {
			return this.timeoutTickDuration;
		}

		public void setTimeoutTickDuration(Duration timeoutTickDuration) {
			this.timeoutTickDuration = timeoutTickDuration;
		}

	}

	public static class Chunking {

		/**
		 * Whether to automatically drop outstanding uncompleted chunked messages once the
		 * consumer queue reaches the threshold set by the 'maxPendingMessages' property.
		 */
		private Boolean autoAckOldestOnQueueFull = true;

		/**
		 * The maximum time period for a consumer to receive all chunks of a message - if
		 * this threshold is exceeded the consumer will expire the incomplete chunks.
		 */
		private Duration expireTimeIncomplete = Duration.ofMinutes(1);

		/**
		 * Maximum number of chunked messages to be kept in memory.
		 */
		private Integer maxPendingMessages = 10;

		public Boolean getAutoAckOldestOnQueueFull() {
			return this.autoAckOldestOnQueueFull;
		}

		public void setAutoAckOldestOnQueueFull(Boolean autoAckOldestOnQueueFull) {
			this.autoAckOldestOnQueueFull = autoAckOldestOnQueueFull;
		}

		public Duration getExpireTimeIncomplete() {
			return this.expireTimeIncomplete;
		}

		public void setExpireTimeIncomplete(Duration expireTimeIncomplete) {
			this.expireTimeIncomplete = expireTimeIncomplete;
		}

		public Integer getMaxPendingMessages() {
			return this.maxPendingMessages;
		}

		public void setMaxPendingMessages(Integer maxPendingMessages) {
			this.maxPendingMessages = maxPendingMessages;
		}

	}

	public static class Subscription extends PulsarProperties.Consumer.Subscription {

		/**
		 * Map of properties to add to the subscription.
		 */
		private Map<String, String> properties = new HashMap<>();

		/**
		 * Whether to replicate subscription state.
		 */
		private Boolean replicateState = false;

		public Map<String, String> getProperties() {
			return this.properties;
		}

		public void setProperties(Map<String, String> properties) {
			this.properties = properties;
		}

		public Boolean getReplicateState() {
			return this.replicateState;
		}

		public void setReplicateState(Boolean replicateState) {
			this.replicateState = replicateState;
		}

	}

	static class Properties extends HashMap<String, Object> {

		<V> java.util.function.Consumer<V> in(String key) {
			return (value) -> put(key, value);
		}

	}

}
