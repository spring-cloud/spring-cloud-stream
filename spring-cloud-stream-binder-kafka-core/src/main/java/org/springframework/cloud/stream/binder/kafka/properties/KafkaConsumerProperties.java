/*
 * Copyright 2016-2019 the original author or authors.
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

	private boolean ackEachRecord;

	private boolean autoRebalanceEnabled = true;

	private boolean autoCommitOffset = true;

	private Boolean autoCommitOnError;

	private StartOffset startOffset;

	private boolean resetOffsets;

	private boolean enableDlq;

	private String dlqName;

	private Integer dlqPartitions;

	private KafkaProducerProperties dlqProducerProperties = new KafkaProducerProperties();

	private int recoveryInterval = 5000;

	private String[] trustedPackages;

	private StandardHeaders standardHeaders = StandardHeaders.none;

	private String converterBeanName;

	private long idleEventInterval = 30_000;

	private boolean destinationIsPattern;

	private Map<String, String> configuration = new HashMap<>();

	private KafkaTopicProperties topic = new KafkaTopicProperties();

	/**
	 * Timeout used for polling in pollable consumers.
	 */
	private long pollTimeout = org.springframework.kafka.listener.ConsumerProperties.DEFAULT_POLL_TIMEOUT;

	public boolean isAckEachRecord() {
		return this.ackEachRecord;
	}

	public void setAckEachRecord(boolean ackEachRecord) {
		this.ackEachRecord = ackEachRecord;
	}

	public boolean isAutoCommitOffset() {
		return this.autoCommitOffset;
	}

	public void setAutoCommitOffset(boolean autoCommitOffset) {
		this.autoCommitOffset = autoCommitOffset;
	}

	public StartOffset getStartOffset() {
		return this.startOffset;
	}

	public void setStartOffset(StartOffset startOffset) {
		this.startOffset = startOffset;
	}

	public boolean isResetOffsets() {
		return this.resetOffsets;
	}

	public void setResetOffsets(boolean resetOffsets) {
		this.resetOffsets = resetOffsets;
	}

	public boolean isEnableDlq() {
		return this.enableDlq;
	}

	public void setEnableDlq(boolean enableDlq) {
		this.enableDlq = enableDlq;
	}

	public Boolean getAutoCommitOnError() {
		return this.autoCommitOnError;
	}

	public void setAutoCommitOnError(Boolean autoCommitOnError) {
		this.autoCommitOnError = autoCommitOnError;
	}

	/**
	 * No longer used.
	 * @return the interval.
	 * @deprecated No longer used by the binder
	 */
	@Deprecated
	public int getRecoveryInterval() {
		return this.recoveryInterval;
	}

	/**
	 * No longer used.
	 * @param recoveryInterval the interval.
	 * @deprecated No longer needed by the binder
	 */
	@Deprecated
	public void setRecoveryInterval(int recoveryInterval) {
		this.recoveryInterval = recoveryInterval;
	}

	public boolean isAutoRebalanceEnabled() {
		return this.autoRebalanceEnabled;
	}

	public void setAutoRebalanceEnabled(boolean autoRebalanceEnabled) {
		this.autoRebalanceEnabled = autoRebalanceEnabled;
	}

	public Map<String, String> getConfiguration() {
		return this.configuration;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}

	public String getDlqName() {
		return this.dlqName;
	}

	public void setDlqName(String dlqName) {
		this.dlqName = dlqName;
	}

	public Integer getDlqPartitions() {
		return this.dlqPartitions;
	}

	public void setDlqPartitions(Integer dlqPartitions) {
		this.dlqPartitions = dlqPartitions;
	}

	public String[] getTrustedPackages() {
		return this.trustedPackages;
	}

	public void setTrustedPackages(String[] trustedPackages) {
		this.trustedPackages = trustedPackages;
	}

	public KafkaProducerProperties getDlqProducerProperties() {
		return this.dlqProducerProperties;
	}

	public void setDlqProducerProperties(KafkaProducerProperties dlqProducerProperties) {
		this.dlqProducerProperties = dlqProducerProperties;
	}

	public StandardHeaders getStandardHeaders() {
		return this.standardHeaders;
	}

	public void setStandardHeaders(StandardHeaders standardHeaders) {
		this.standardHeaders = standardHeaders;
	}

	public String getConverterBeanName() {
		return this.converterBeanName;
	}

	public void setConverterBeanName(String converterBeanName) {
		this.converterBeanName = converterBeanName;
	}

	public long getIdleEventInterval() {
		return this.idleEventInterval;
	}

	public void setIdleEventInterval(long idleEventInterval) {
		this.idleEventInterval = idleEventInterval;
	}

	public boolean isDestinationIsPattern() {
		return this.destinationIsPattern;
	}

	public void setDestinationIsPattern(boolean destinationIsPattern) {
		this.destinationIsPattern = destinationIsPattern;
	}

	public KafkaTopicProperties getTopic() {
		return this.topic;
	}

	public void setTopic(KafkaTopicProperties topic) {
		this.topic = topic;
	}

	public long getPollTimeout() {
		return this.pollTimeout;
	}

	public void setPollTimeout(long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}
}
