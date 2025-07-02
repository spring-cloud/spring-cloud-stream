/*
 * Copyright 2023-2025 the original author or authors.
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.client.api.ProducerCryptoFailureAction;

import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.boot.pulsar.autoconfigure.PulsarProperties;
import org.springframework.lang.Nullable;
import org.springframework.util.unit.DataSize;

/**
 * Configuration properties used to specify Pulsar producers.
 *
 * @author Chris Bono
 */
public class ProducerConfigProperties extends PulsarProperties.Producer {

	/**
	 * Whether the "send" and "sendAsync" methods should block if the outgoing message
	 * queue is full.
	 */
	private Boolean blockIfQueueFull = false;

	/**
	 * Maximum number of pending messages for the producer.
	 */
	private Integer maxPendingMessages = 1000;

	/**
	 * Maximum number of pending messages across all the partitions.
	 */
	private Integer maxPendingMessagesAcrossPartitions = 50000;

	/**
	 * Action the producer will take in case of encryption failure.
	 */
	private ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.FAIL;

	/**
	 * Names of the public encryption keys to use when encrypting data.
	 */
	private Set<String> encryptionKeys = new HashSet<>();

	/**
	 * Baseline for the sequence ids for messages published by the producer.
	 */
	@Nullable
	private Long initialSequenceId;

	/**
	 * Whether partitioned producer automatically discover new partitions at runtime.
	 */
	private Boolean autoUpdatePartitions = true;

	/**
	 * Interval of partitions discovery updates.
	 */
	private Duration autoUpdatePartitionsInterval = Duration.ofMinutes(1);

	/**
	 * Whether the multiple schema mode is enabled.
	 */
	private Boolean multiSchema = true;

	/**
	 * Whether producers in Shared mode register and connect immediately to the owner
	 * broker of each partition or start lazily on demand.
	 */
	private Boolean lazyStartPartitionedProducers = false;

	private final Batching batch = new Batching();

	public Batching getBatch() {
		return this.batch;
	}

	/**
	 * Map of properties to add to the producer.
	 */
	private Map<String, String> properties = new HashMap<>();

	public Boolean getBlockIfQueueFull() {
		return this.blockIfQueueFull;
	}

	public void setBlockIfQueueFull(Boolean blockIfQueueFull) {
		this.blockIfQueueFull = blockIfQueueFull;
	}

	public Integer getMaxPendingMessages() {
		return this.maxPendingMessages;
	}

	public void setMaxPendingMessages(Integer maxPendingMessages) {
		this.maxPendingMessages = maxPendingMessages;
	}

	public Integer getMaxPendingMessagesAcrossPartitions() {
		return this.maxPendingMessagesAcrossPartitions;
	}

	public void setMaxPendingMessagesAcrossPartitions(Integer maxPendingMessagesAcrossPartitions) {
		this.maxPendingMessagesAcrossPartitions = maxPendingMessagesAcrossPartitions;
	}

	public ProducerCryptoFailureAction getCryptoFailureAction() {
		return this.cryptoFailureAction;
	}

	public void setCryptoFailureAction(ProducerCryptoFailureAction cryptoFailureAction) {
		this.cryptoFailureAction = cryptoFailureAction;
	}

	public Set<String> getEncryptionKeys() {
		return this.encryptionKeys;
	}

	public void setEncryptionKeys(Set<String> encryptionKeys) {
		this.encryptionKeys = encryptionKeys;
	}

	@Nullable
	public Long getInitialSequenceId() {
		return this.initialSequenceId;
	}

	public void setInitialSequenceId(@Nullable Long initialSequenceId) {
		this.initialSequenceId = initialSequenceId;
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

	public Boolean getMultiSchema() {
		return this.multiSchema;
	}

	public void setMultiSchema(Boolean multiSchema) {
		this.multiSchema = multiSchema;
	}

	public Boolean getLazyStartPartitionedProducers() {
		return this.lazyStartPartitionedProducers;
	}

	public void setLazyStartPartitionedProducers(Boolean lazyStartPartitionedProducers) {
		this.lazyStartPartitionedProducers = lazyStartPartitionedProducers;
	}

	public Map<String, String> getProperties() {
		return this.properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	/**
	 * Gets a map representation of the base producer properties (those defined in parent
	 * class).
	 * @return map of base producer properties and associated values.
	 */
	public Map<String, Object> toBaseProducerPropertiesMap() {
		var producerProps = new ProducerConfigProperties.Properties();
		var map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		map.from(this::getAccessMode).to(producerProps.in("accessMode"));
		map.from(this::isBatchingEnabled).to(producerProps.in("batchingEnabled"));
		map.from(this::isChunkingEnabled).to(producerProps.in("chunkingEnabled"));
		map.from(this::getCompressionType).to(producerProps.in("compressionType"));
		map.from(this::getHashingScheme).to(producerProps.in("hashingScheme"));
		map.from(this::getMessageRoutingMode).to(producerProps.in("messageRoutingMode"));
		map.from(this::getName).to(producerProps.in("producerName"));
		map.from(this::getSendTimeout).asInt(Duration::toMillis).to(producerProps.in("sendTimeoutMs"));
		map.from(this::getTopicName).to(producerProps.in("topicName"));
		return producerProps;
	}

	/**
	 * Gets a map representation of the extended producer properties (those defined in
	 * this class).
	 * @return map of extended producer properties and associated values.
	 */
	public Map<String, Object> toExtendedProducerPropertiesMap() {
		var producerProps = new ProducerConfigProperties.Properties();
		var map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		map.from(this::getAutoUpdatePartitions).to(producerProps.in("autoUpdatePartitions"));
		map.from(this::getAutoUpdatePartitionsInterval).as(Duration::toSeconds)
				.to(producerProps.in("autoUpdatePartitionsIntervalSeconds"));
		map.from(this::getBlockIfQueueFull).to(producerProps.in("blockIfQueueFull"));
		map.from(this::getCryptoFailureAction).to(producerProps.in("cryptoFailureAction"));
		map.from(this::getEncryptionKeys).to(producerProps.in("encryptionKeys"));
		map.from(this::getInitialSequenceId).to(producerProps.in("initialSequenceId"));
		map.from(this::getLazyStartPartitionedProducers).to(producerProps.in("lazyStartPartitionedProducers"));
		map.from(this::getMaxPendingMessages).to(producerProps.in("maxPendingMessages"));
		map.from(this::getMaxPendingMessagesAcrossPartitions)
				.to(producerProps.in("maxPendingMessagesAcrossPartitions"));
		map.from(this::getMultiSchema).to(producerProps.in("multiSchema"));
		map.from(this::getProperties).to(producerProps.in("properties"));
		this.mapBatchProperties(this.getBatch(), producerProps, map);
		return producerProps;
	}

	private void mapBatchProperties(Batching batch, Properties producerProps, PropertyMapper map) {
		if (this.isBatchingEnabled()) {
			map.from(batch::getMaxPublishDelay).as(it -> it.toNanos() / 1000)
					.to(producerProps.in("batchingMaxPublishDelayMicros"));
			map.from(batch::getPartitionSwitchFrequencyByPublishDelay)
					.to(producerProps.in("batchingPartitionSwitchFrequencyByPublishDelay"));
			map.from(batch::getMaxMessages).to(producerProps.in("batchingMaxMessages"));
			map.from(batch::getMaxBytes).asInt(DataSize::toBytes)
					.to(producerProps.in("batchingMaxBytes"));
		}
	}

	/**
	 * Gets a map representation of base and extended producer properties.
	 * @return map of base and extended producer properties and associated values.
	 */
	public Map<String, Object> toAllProducerPropertiesMap() {
		var producerProps = this.toBaseProducerPropertiesMap();
		producerProps.putAll(this.toExtendedProducerPropertiesMap());
		return producerProps;
	}

	public static class Batching {

		/**
		 * Time period within which the messages sent will be batched.
		 */
		private Duration maxPublishDelay = Duration.ofMillis(1);

		/**
		 * Partition switch frequency while batching of messages is enabled and using
		 * round-robin routing mode for non-keyed message.
		 */
		private Integer partitionSwitchFrequencyByPublishDelay = 10;

		/**
		 * Maximum number of messages to be batched.
		 */
		private Integer maxMessages = 1000;

		/**
		 * Maximum number of bytes permitted in a batch.
		 */
		private DataSize maxBytes = DataSize.ofKilobytes(128);

		/**
		 * Whether to automatically batch messages.
		 */
		private Boolean enabled = true;

		public Duration getMaxPublishDelay() {
			return this.maxPublishDelay;
		}

		public void setMaxPublishDelay(Duration maxPublishDelay) {
			this.maxPublishDelay = maxPublishDelay;
		}

		public Integer getPartitionSwitchFrequencyByPublishDelay() {
			return this.partitionSwitchFrequencyByPublishDelay;
		}

		public void setPartitionSwitchFrequencyByPublishDelay(Integer partitionSwitchFrequencyByPublishDelay) {
			this.partitionSwitchFrequencyByPublishDelay = partitionSwitchFrequencyByPublishDelay;
		}

		public Integer getMaxMessages() {
			return this.maxMessages;
		}

		public void setMaxMessages(Integer maxMessages) {
			this.maxMessages = maxMessages;
		}

		public DataSize getMaxBytes() {
			return this.maxBytes;
		}

		public void setMaxBytes(DataSize maxBytes) {
			this.maxBytes = maxBytes;
		}

		public Boolean getEnabled() {
			return this.enabled;
		}

		public void setEnabled(Boolean enabled) {
			this.enabled = enabled;
		}

	}

	static class Properties extends HashMap<String, Object> {

		<V> java.util.function.Consumer<V> in(String key) {
			return (value) -> put(key, value);
		}

	}

}
