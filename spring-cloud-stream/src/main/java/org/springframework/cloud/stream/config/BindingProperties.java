/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Contains the properties of a binding.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
@JsonInclude(value = Include.NON_DEFAULT)
public class BindingProperties {

	private static final String COMMA = ",";

	/**
	 * The physical name at the broker that the binder binds to.
	 */
	private String destination;

	/**
	 * Unique name that the binding belongs to (applies to consumers only). Multiple consumers within the same group
	 * share the subscription. A null or empty String value indicates an anonymous group that is not shared.
	 *
	 * @see org.springframework.cloud.stream.binder.Binder#bindConsumer(java.lang.String, java.lang.String,
	 * java.lang.Object, java.util.Properties)
	 */
	private String group;

	// Properties for both inbound/outbound

	private String contentType;

	private String binder;

	private Boolean trackHistory;

	// Outbound properties

	// Partition properties

	private String partitionKeyExpression;

	private String partitionKeyExtractorClass;

	private String partitionSelectorClass;

	private String partitionSelectorExpression;

	private Integer partitionCount = 1;

	private Integer nextModuleCount;

	private Integer nextModuleConcurrency;

	// Batching properties
	private Boolean batchingEnabled;

	private Integer batchSize;

	private Integer batchBufferLimit;

	private Integer batchTimeout;


	// Inbound properties
	private Integer concurrency;

	// Partition properties
	private String partitionIndex;

	private Boolean partitioned = false;

	// Rabbit specific consumer properties
	private Boolean durableSubscription;

	public String getDestination() {
		return this.destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getContentType() {
		return this.contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getBinder() {
		return binder;
	}

	public void setBinder(String binder) {
		this.binder = binder;
	}

	public Boolean isTrackHistory() {
		return this.trackHistory;
	}

	public void setTrackHistory(Boolean trackHistory) {
		this.trackHistory = trackHistory;
	}

	public String getPartitionKeyExpression() {
		return this.partitionKeyExpression;
	}

	public void setPartitionKeyExpression(String partitionKeyExpression) {
		this.partitionKeyExpression = partitionKeyExpression;
	}

	public String getPartitionKeyExtractorClass() {
		return this.partitionKeyExtractorClass;
	}

	public void setPartitionKeyExtractorClass(String partitionKeyExtractorClass) {
		this.partitionKeyExtractorClass = partitionKeyExtractorClass;
	}

	public String getPartitionSelectorClass() {
		return this.partitionSelectorClass;
	}

	public void setPartitionSelectorClass(String partitionSelectorClass) {
		this.partitionSelectorClass = partitionSelectorClass;
	}

	public String getPartitionSelectorExpression() {
		return this.partitionSelectorExpression;
	}

	public void setPartitionSelectorExpression(String partitionSelectorExpression) {
		this.partitionSelectorExpression = partitionSelectorExpression;
	}

	public Integer getNextModuleCount() {
		return this.nextModuleCount;
	}

	public void setNextModuleCount(Integer nextModuleCount) {
		this.nextModuleCount = nextModuleCount;
	}

	public Integer getNextModuleConcurrency() {
		return this.nextModuleConcurrency;
	}

	public void setNextModuleConcurrency(Integer nextModuleConcurrency) {
		this.nextModuleConcurrency = nextModuleConcurrency;
	}

	public Boolean isBatchingEnabled() {
		return this.batchingEnabled;
	}

	public void setBatchingEnabled(Boolean batchingEnabled) {
		this.batchingEnabled = batchingEnabled;
	}

	public Integer getBatchSize() {
		return this.batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public Integer getBatchBufferLimit() {
		return this.batchBufferLimit;
	}

	public void setBatchBufferLimit(Integer batchBufferLimit) {
		this.batchBufferLimit = batchBufferLimit;
	}

	public Integer getBatchTimeout() {
		return this.batchTimeout;
	}

	public void setBatchTimeout(Integer batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public Integer getPartitionCount() {
		return this.partitionCount;
	}

	public void setPartitionCount(Integer partitionCount) {
		this.partitionCount = partitionCount;
	}

	public Integer getConcurrency() {
		return this.concurrency;
	}

	public void setConcurrency(Integer concurrency) {
		this.concurrency = concurrency;
	}

	public String getPartitionIndex() {
		return this.partitionIndex;
	}

	public void setPartitionIndex(String partitionIndex) {
		this.partitionIndex = partitionIndex;
	}

	public Boolean isPartitioned() {
		return this.partitioned;
	}

	public void setPartitioned(Boolean partitioned) {
		this.partitioned = partitioned;
	}

	public Boolean isDurableSubscription() {
		return this.durableSubscription;
	}

	public void setDurableSubscription(Boolean durableSubscription) {
		this.durableSubscription = durableSubscription;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("destination=" + this.destination);
		sb.append(COMMA);
		sb.append("group=" + this.group);
		sb.append(COMMA);
		if (this.contentType != null) {
			sb.append("contentType=" + this.contentType);
			sb.append(COMMA);
		}
		if (this.binder != null) {
			sb.append("binder=" + this.binder);
			sb.append(COMMA);
		}
		if (this.trackHistory != null) {
			sb.append("trackHistory=" + this.trackHistory);
			sb.append(COMMA);
		}
		if (this.partitionKeyExpression != null && !this.partitionKeyExpression.isEmpty()) {
			sb.append("partitionKeyExpression=" + partitionKeyExpression);
			sb.append(COMMA);
		}
		if (this.partitionKeyExtractorClass != null && !this.partitionKeyExtractorClass.isEmpty()) {
			sb.append("partitionKeyExtractorClass=" + partitionKeyExtractorClass);
			sb.append(COMMA);
		}
		if (this.partitionSelectorClass != null && !this.partitionSelectorClass.isEmpty()) {
			sb.append("partitionSelectorClass=" + partitionSelectorClass);
			sb.append(COMMA);
		}
		if (this.partitionSelectorExpression != null && !this.partitionSelectorExpression.isEmpty()) {
			sb.append("partitionSelectorExpression=" + partitionSelectorExpression);
			sb.append(COMMA);
		}
		if (this.partitionCount != null) {
			sb.append("partitionCount=" + this.partitionCount);
			sb.append(COMMA);
		}
		if (this.nextModuleCount != null) {
			sb.append("nextModuleCount=" + this.nextModuleCount);
			sb.append(COMMA);
		}
		if (this.nextModuleConcurrency != null) {
			sb.append("nextModuleConcurrency=" + this.nextModuleConcurrency);
			sb.append(COMMA);
		}
		if (this.batchingEnabled != null) {
			sb.append("batchingEnabled=" + this.batchingEnabled);
			sb.append(COMMA);
		}
		if (this.batchSize != null) {
			sb.append("batchSize=" + this.batchSize);
			sb.append(COMMA);
		}
		if (this.batchBufferLimit != null) {
			sb.append("batchBufferLimit=" + this.batchBufferLimit);
			sb.append(COMMA);
		}
		if (this.batchTimeout != null) {
			sb.append("batchTimeout=" + this.batchTimeout);
			sb.append(COMMA);
		}
		if (this.partitioned != null) {
			sb.append("partitioned=" + this.partitioned);
			sb.append(COMMA);
		}
		if (this.partitionIndex != null) {
			sb.append("partitionIndex=" + this.partitionIndex);
			sb.append(COMMA);
		}
		if (this.concurrency != null) {
			sb.append("concurrency=" + this.concurrency);
			sb.append(COMMA);
		}
		if (this.durableSubscription != null) {
			sb.append("durableSubscription=" + this.durableSubscription);
			sb.append(COMMA);
		}
		sb.deleteCharAt(sb.lastIndexOf(COMMA));
		return "BindingProperties{" + sb.toString() + "}";
	}

}
