/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Soby Chacko
 * @author Gary Russell
 *
 * <p>
 * Thanks to Laszlo Szabo for providing the initial patch for generic property support.
 * </p>
 */
public class KafkaConsumerProperties {

	public enum StartOffset {
		earliest(-2L),
		latest(-1L);

		private final long referencePoint;

		StartOffset(long referencePoint) {
			this.referencePoint = referencePoint;
		}

		public long getReferencePoint() {
			return this.referencePoint;
		}
	}

	public enum StandardHeaders {
		none,
		id,
		timestamp,
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

	private KafkaProducerProperties dlqProducerProperties = new KafkaProducerProperties();

	private int recoveryInterval = 5000;

	private String[] trustedPackages;

	private StandardHeaders standardHeaders = StandardHeaders.none;

	private String converterBeanName;

	private long idleEventInterval = 30_000;

	private boolean destinationIsPattern;

	private Map<String, String> configuration = new HashMap<>();

	private KafkaAdminProperties admin = new KafkaAdminProperties();

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
	 * @deprecated
	 */
	@Deprecated
	public int getRecoveryInterval() {
		return this.recoveryInterval;
	}

	/**
	 * No longer used.
	 * @param recoveryInterval the interval.
	 * @deprecated
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
		return dlqName;
	}

	public void setDlqName(String dlqName) {
		this.dlqName = dlqName;
	}

	public String[] getTrustedPackages() {
		return trustedPackages;
	}

	public void setTrustedPackages(String[] trustedPackages) {
		this.trustedPackages = trustedPackages;
	}

	public KafkaProducerProperties getDlqProducerProperties() {
		return dlqProducerProperties;
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

	public KafkaAdminProperties getAdmin() {
		return this.admin;
	}

	public void setAdmin(KafkaAdminProperties admin) {
		this.admin = admin;
	}

}
