/*
 * Copyright 2015 the original author or authors.
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

import java.util.UUID;

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

	private String destination;

	private boolean partitioned = false;

	private int partitionCount = 1;

	private String partitionKeyExpression;

	private String partitionKeyExtractorClass;

	private String partitionSelectorClass;

	private String partitionSelectorExpression;

	private String group = UUID.randomUUID().toString();

	private String contentType;

	private String binder;

	private boolean trackHistory;

	public String getDestination() {
		return this.destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public boolean isPartitioned() {
		return this.partitioned;
	}

	public void setPartitioned(boolean partitioned) {
		this.partitioned = partitioned;
	}

	public int getPartitionCount() {
		return this.partitionCount;
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
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

	public Boolean getTrackHistory() {
		return this.trackHistory;
	}

	public void setTrackHistory(boolean trackHistory) {
		this.trackHistory = trackHistory;
	}

}
