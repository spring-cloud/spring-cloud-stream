/*
 * Copyright 2016-2017 the original author or authors.
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

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.springframework.expression.Expression;

/**
 * Common producer properties.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
@JsonInclude(Include.NON_DEFAULT)
public class ProducerProperties {

	@JsonSerialize(using = ExpressionSerializer.class)
	private Expression partitionKeyExpression;

	private Class<?> partitionKeyExtractorClass;

	private Class<?> partitionSelectorClass;

	@JsonSerialize(using = ExpressionSerializer.class)
	private Expression partitionSelectorExpression;

	private int partitionCount = 1;

	private String[] requiredGroups = new String[] {};

	private HeaderMode headerMode;

	private boolean useNativeEncoding = false;

	private boolean errorChannelEnabled = false;

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

	@Min(value = 1, message = "Partition count should be greater than zero.")
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

	@AssertTrue(message = "Partition key expression and partition key extractor class properties are mutually exclusive.")
	public boolean isValidPartitionKeyProperty() {
		return (this.partitionKeyExpression == null) || (this.partitionKeyExtractorClass == null);
	}

	@AssertTrue(message = "Partition selector class and partition selector expression properties are mutually exclusive.")
	public boolean isValidPartitionSelectorProperty() {
		return (this.partitionSelectorClass == null) || (this.partitionSelectorExpression == null);
	}

	public HeaderMode getHeaderMode() {
		return this.headerMode;
	}

	public void setHeaderMode(HeaderMode headerMode) {
		this.headerMode = headerMode;
	}

	public boolean isUseNativeEncoding() {
		return this.useNativeEncoding;
	}

	public void setUseNativeEncoding(boolean useNativeEncoding) {
		this.useNativeEncoding = useNativeEncoding;
	}

	public boolean isErrorChannelEnabled() {
		return this.errorChannelEnabled;
	}

	public void setErrorChannelEnabled(boolean errorChannelEnabled) {
		this.errorChannelEnabled = errorChannelEnabled;
	}

}
