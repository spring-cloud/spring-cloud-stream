/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import org.springframework.expression.Expression;

/**
 * Common producer properties.
 *
 * @author Marius Bogoevici
 */
public class ProducerProperties {

	private Expression partitionKeyExpression = null;

	private Class<?> partitionKeyExtractorClass = null;

	private Class<?> partitionSelectorClass = null;

	private Expression partitionSelectorExpression = null;

	private boolean batchingEnabled = false;

	private int batchSize;

	private int batchBufferLimit;

	private int batchTimeout;

	private int partitionCount = 1;

	private String[] requiredGroups = new String[] {};

	public Expression getPartitionKeyExpression() {
		return partitionKeyExpression;
	}

	public void setPartitionKeyExpression(Expression partitionKeyExpression) {
		this.partitionKeyExpression = partitionKeyExpression;
	}

	public Class<?> getPartitionKeyExtractorClass() {
		return partitionKeyExtractorClass;
	}

	public void setPartitionKeyExtractorClass(Class<?> partitionKeyExtractorClass) {
		this.partitionKeyExtractorClass = partitionKeyExtractorClass;
	}

	public boolean isPartitioned() {
		return this.partitionKeyExpression != null || partitionKeyExtractorClass != null;
	}

	public Class<?> getPartitionSelectorClass() {
		return partitionSelectorClass;
	}

	public void setPartitionSelectorClass(Class<?> partitionSelectorClass) {
		this.partitionSelectorClass = partitionSelectorClass;
	}

	public Expression getPartitionSelectorExpression() {
		return partitionSelectorExpression;
	}

	public void setPartitionSelectorExpression(Expression partitionSelectorExpression) {
		this.partitionSelectorExpression = partitionSelectorExpression;
	}

	public boolean isBatchingEnabled() {
		return batchingEnabled;
	}

	public void setBatchingEnabled(boolean batchingEnabled) {
		this.batchingEnabled = batchingEnabled;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getBatchBufferLimit() {
		return batchBufferLimit;
	}

	public void setBatchBufferLimit(int batchBufferLimit) {
		this.batchBufferLimit = batchBufferLimit;
	}

	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}

	public String[] getRequiredGroups() {
		return requiredGroups;
	}

	public void setRequiredGroups(String... requiredGroups) {
		this.requiredGroups = requiredGroups;
	}

}
