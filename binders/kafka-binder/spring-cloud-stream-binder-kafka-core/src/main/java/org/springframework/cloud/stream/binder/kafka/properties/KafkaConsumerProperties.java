/*
 * Copyright 2016-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.properties;

import java.util.HashMap;
import java.util.Map;

import org.springframework.kafka.listener.ContainerProperties;

/**
 * Extended consumer properties for Kafka binder.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Soby Chacko
 * @author Gary Russell
 * @author Aldo Sinanaj
 *
 * <p>
 * Thanks to Laszlo Szabo for providing the initial patch for generic property support.
 * </p>
 */
public class KafkaConsumerProperties {

	/**
	 * Enumeration for starting consumer offset.
	 */
	public enum StartOffset {

		/**
		 * Starting from earliest offset.
		 */
		earliest(-2L),
		/**
		 * Starting from latest offset.
		 */
		latest(-1L);

		private final long referencePoint;

		StartOffset(long referencePoint) {
			this.referencePoint = referencePoint;
		}

		public long getReferencePoint() {
			return this.referencePoint;
		}

	}

	/**
	 * Standard headers for the message.
	 */
	public enum StandardHeaders {

		/**
		 * No headers.
		 */
		none,
		/**
		 * Message header representing ID.
		 */
		id,
		/**
		 * Message header representing timestamp.
		 */
		timestamp,
		/**
		 * Indicating both ID and timestamp headers.
		 */
		both

	}


	/**
	 * When true, topic partitions is automatically rebalanced between the members of a consumer group.
	 * When false, each consumer is assigned a fixed set of partitions based on spring.cloud.stream.instanceCount and spring.cloud.stream.instanceIndex.
	 */
	private boolean autoRebalanceEnabled = true;

	/**
	 * Controlling the container acknowledgement mode. This is the preferred way to control the ack mode on the
	 * container instead of the deprecated autoCommitOffset property.
	 */
	private ContainerProperties.AckMode ackMode;

	/**
	 * Flag to enable auto commit on error in polled consumers.
	 */
	private Boolean autoCommitOnError;

	/**
	 * The starting offset for new groups. Allowed values: earliest and latest.
	 */
	private StartOffset startOffset;

	/**
	 * Whether to reset offsets on the consumer to the value provided by startOffset.
	 * Must be false if a KafkaRebalanceListener is provided.
	 */
	private boolean resetOffsets;

	/**
	 * When set to true, it enables DLQ behavior for the consumer.
	 * By default, messages that result in errors are forwarded to a topic named error.name-of-destination.name-of-group.
	 * The DLQ topic name can be configurable by setting the dlqName property.
	 */
	private boolean enableDlq;

	/**
	 * The name of the DLQ topic to receive the error messages.
	 */
	private String dlqName;

	/**
	 * Number of partitions to use on the DLQ.
	 */
	private Integer dlqPartitions;

	/**
	 * Using this, DLQ-specific producer properties can be set.
	 * All the properties available through kafka producer properties can be set through this property.
	 */
	private KafkaProducerProperties dlqProducerProperties = new KafkaProducerProperties();

	/**
	 * List of trusted packages to provide the header mapper.
	 */
	private String[] trustedPackages;

	/**
	 * Indicates which standard headers are populated by the inbound channel adapter.
	 * Allowed values: none, id, timestamp, or both.
	 */
	private StandardHeaders standardHeaders = StandardHeaders.none;

	/**
	 * The name of a bean that implements RecordMessageConverter.
	 */
	private String converterBeanName;

	/**
	 * The interval, in milliseconds, between events indicating that no messages have recently been received.
	 */
	private long idleEventInterval = 30_000;

	/**
	 * When true, the destination is treated as a regular expression Pattern used to match topic names by the broker.
	 */
	private boolean destinationIsPattern;

	/**
	 * Map with a key/value pair containing generic Kafka consumer properties.
	 * In addition to having Kafka consumer properties, other configuration properties can be passed here.
	 */
	private Map<String, String> configuration = new HashMap<>();

	/**
	 * Various topic level properties. @see {@link KafkaTopicProperties} for more details.
	 */
	private KafkaTopicProperties topic = new KafkaTopicProperties();

	/**
	 * Timeout used for polling in pollable consumers.
	 */
	private long pollTimeout = org.springframework.kafka.listener.ConsumerProperties.DEFAULT_POLL_TIMEOUT;

	/**
	 * Transaction manager bean name - overrides the binder's transaction configuration.
	 */
	private String transactionManager;

	/**
	 * Set to false to NOT commit the offset of a successfully recovered recovered in the after rollback processor.
	 */
	private boolean txCommitRecovered = true;

	/**
	 * CommonErrorHandler bean name per consumer binding.
	 * @since 3.2
	 */
	private String commonErrorHandlerBeanName;

	/**
	 * When using the reactive binder, automatically commit offsets when records received
	 * by the poll have been processed.
	 * @since 4.0.2
	 */
	private boolean reactiveAutoCommit;

	/**
	 * When using the reactive binder, automatically commit offsets of records before they
	 * are passed to the user code.
	 * @since 4.0.3
	 */
	private boolean reactiveAtMostOnce;

	/**
	 * @return Container's ack mode.
	 */
	public ContainerProperties.AckMode getAckMode() {
		return this.ackMode;
	}

	public void setAckMode(ContainerProperties.AckMode ackMode) {
		this.ackMode = ackMode;
	}

	/**
	 * @return start offset
	 *
	 * The starting offset for new groups. Allowed values: earliest and latest.
	 */
	public StartOffset getStartOffset() {
		return this.startOffset;
	}

	public void setStartOffset(StartOffset startOffset) {
		this.startOffset = startOffset;
	}

	/**
	 * @return if resetting offset is enabled
	 *
	 * Whether to reset offsets on the consumer to the value provided by startOffset.
	 * Must be false if a KafkaRebalanceListener is provided.
	 */
	public boolean isResetOffsets() {
		return this.resetOffsets;
	}

	public void setResetOffsets(boolean resetOffsets) {
		this.resetOffsets = resetOffsets;
	}

	/**
	 * @return is DLQ enabled.
	 *
	 * When set to true, it enables DLQ behavior for the consumer.
	 * By default, messages that result in errors are forwarded to a topic named error.name-of-destination.name-of-group.
	 * The DLQ topic name can be configurable by setting the dlqName property.
	 */
	public boolean isEnableDlq() {
		return this.enableDlq;
	}

	public void setEnableDlq(boolean enableDlq) {
		this.enableDlq = enableDlq;
	}

	/**
	 * @return is autocommit on error in polled consumers.
	 *
	 * This property accessor is only used in polled consumers.
	 */
	public Boolean getAutoCommitOnError() {
		return this.autoCommitOnError;
	}

	/**
	 *
	 * @param autoCommitOnError commit on error in polled consumers.
	 *
	 */
	public void setAutoCommitOnError(Boolean autoCommitOnError) {
		this.autoCommitOnError = autoCommitOnError;
	}

	/**
	 * @return is auto rebalance enabled
	 *
	 * When true, topic partitions is automatically rebalanced between the members of a consumer group.
	 * When false, each consumer is assigned a fixed set of partitions based on spring.cloud.stream.instanceCount and spring.cloud.stream.instanceIndex.
	 */
	public boolean isAutoRebalanceEnabled() {
		return this.autoRebalanceEnabled;
	}

	public void setAutoRebalanceEnabled(boolean autoRebalanceEnabled) {
		this.autoRebalanceEnabled = autoRebalanceEnabled;
	}

	/**
	 * @return a map of configuration
	 *
	 * Map with a key/value pair containing generic Kafka consumer properties.
	 * In addition to having Kafka consumer properties, other configuration properties can be passed here.
	 */
	public Map<String, String> getConfiguration() {
		return this.configuration;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}

	/**
	 * @return dlq name
	 *
	 * The name of the DLQ topic to receive the error messages.
	 */
	public String getDlqName() {
		return this.dlqName;
	}

	public void setDlqName(String dlqName) {
		this.dlqName = dlqName;
	}

	/**
	 * @return number of partitions on the DLQ topic
	 *
	 * Number of partitions to use on the DLQ.
	 */
	public Integer getDlqPartitions() {
		return this.dlqPartitions;
	}

	public void setDlqPartitions(Integer dlqPartitions) {
		this.dlqPartitions = dlqPartitions;
	}

	/**
	 * @return trusted packages
	 *
	 * List of trusted packages to provide the header mapper.
	 */
	public String[] getTrustedPackages() {
		return this.trustedPackages;
	}

	public void setTrustedPackages(String[] trustedPackages) {
		this.trustedPackages = trustedPackages;
	}

	/**
	 * @return dlq producer properties
	 *
	 * Using this, DLQ-specific producer properties can be set.
	 * All the properties available through kafka producer properties can be set through this property.
	 */
	public KafkaProducerProperties getDlqProducerProperties() {
		return this.dlqProducerProperties;
	}

	public void setDlqProducerProperties(KafkaProducerProperties dlqProducerProperties) {
		this.dlqProducerProperties = dlqProducerProperties;
	}

	/**
	 * @return standard headers
	 *
	 * Indicates which standard headers are populated by the inbound channel adapter.
	 * Allowed values: none, id, timestamp, or both.
	 */
	public StandardHeaders getStandardHeaders() {
		return this.standardHeaders;
	}

	public void setStandardHeaders(StandardHeaders standardHeaders) {
		this.standardHeaders = standardHeaders;
	}

	/**
	 * @return converter bean name
	 *
	 * The name of a bean that implements RecordMessageConverter.
	 */
	public String getConverterBeanName() {
		return this.converterBeanName;
	}

	public void setConverterBeanName(String converterBeanName) {
		this.converterBeanName = converterBeanName;
	}

	/**
	 * @return idle event interval
	 *
	 * The interval, in milliseconds, between events indicating that no messages have recently been received.
	 */
	public long getIdleEventInterval() {
		return this.idleEventInterval;
	}

	public void setIdleEventInterval(long idleEventInterval) {
		this.idleEventInterval = idleEventInterval;
	}

	/**
	 * @return is destination given through a pattern
	 *
	 * When true, the destination is treated as a regular expression Pattern used to match topic names by the broker.
	 */
	public boolean isDestinationIsPattern() {
		return this.destinationIsPattern;
	}

	public void setDestinationIsPattern(boolean destinationIsPattern) {
		this.destinationIsPattern = destinationIsPattern;
	}

	/**
	 * @return topic properties
	 *
	 * Various topic level properties. @see {@link KafkaTopicProperties} for more details.
	 */
	public KafkaTopicProperties getTopic() {
		return this.topic;
	}

	public void setTopic(KafkaTopicProperties topic) {
		this.topic = topic;
	}

	/**
	 * @return timeout in pollable consumers
	 *
	 * Timeout used for polling in pollable consumers.
	 */
	public long getPollTimeout() {
		return this.pollTimeout;
	}

	public void setPollTimeout(long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	/**
	 * @return the transaction manager bean name.
	 *
	 * Transaction manager bean name (must be {@code KafkaAwareTransactionManager}.
	 */
	public String getTransactionManager() {
		return this.transactionManager;
	}

	public void setTransactionManager(String transactionManager) {
		this.transactionManager = transactionManager;
	}

	public boolean isTxCommitRecovered() {
		return this.txCommitRecovered;
	}

	public void setTxCommitRecovered(boolean txCommitRecovered) {
		this.txCommitRecovered = txCommitRecovered;
	}

	public String getCommonErrorHandlerBeanName() {
		return commonErrorHandlerBeanName;
	}

	public void setCommonErrorHandlerBeanName(String commonErrorHandlerBeanName) {
		this.commonErrorHandlerBeanName = commonErrorHandlerBeanName;
	}

	public boolean isReactiveAutoCommit() {
		return this.reactiveAutoCommit;
	}

	public void setReactiveAutoCommit(boolean reactiveAutoCommit) {
		this.reactiveAutoCommit = reactiveAutoCommit;
	}

	public boolean isReactiveAtMostOnce() {
		return this.reactiveAtMostOnce;
	}

	public void setReactiveAtMostOnce(boolean reactiveAtMostOnce) {
		this.reactiveAtMostOnce = reactiveAtMostOnce;
	}

}
