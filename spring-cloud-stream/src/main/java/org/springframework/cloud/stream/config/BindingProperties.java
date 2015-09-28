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

/**
 * Contains the properties of a binding.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class BindingProperties {

	private String destination;

	private boolean partitioned = false;

	private int partitionCount = 1;

	private String partitionKeyExpression;

	private String partitionKeyExtractorClass;

	private String partitionSelectorClass;

	private String partitionSelectorExpression;

	private String contentType;

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public boolean isPartitioned() {
		return partitioned;
	}

	public void setPartitioned(boolean partitioned) {
		this.partitioned = partitioned;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}

	public String getPartitionKeyExpression() {
		return partitionKeyExpression;
	}

	public void setPartitionKeyExpression(String partitionKeyExpression) {
		this.partitionKeyExpression = partitionKeyExpression;
	}

	public String getPartitionKeyExtractorClass() {
		return partitionKeyExtractorClass;
	}

	public void setPartitionKeyExtractorClass(String partitionKeyExtractorClass) {
		this.partitionKeyExtractorClass = partitionKeyExtractorClass;
	}

	public String getPartitionSelectorClass() {
		return partitionSelectorClass;
	}

	public void setPartitionSelectorClass(String partitionSelectorClass) {
		this.partitionSelectorClass = partitionSelectorClass;
	}

	public String getPartitionSelectorExpression() {
		return partitionSelectorExpression;
	}

	public void setPartitionSelectorExpression(String partitionSelectorExpression) {
		this.partitionSelectorExpression = partitionSelectorExpression;
	}

	public String getContentType() {
		return this.contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

}
