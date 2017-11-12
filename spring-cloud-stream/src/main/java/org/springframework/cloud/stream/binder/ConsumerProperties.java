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

import javax.validation.constraints.Min;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Common consumer properties.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Soby Chacko
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ConsumerProperties {

	private int concurrency = 1;

	private boolean partitioned;

	private int instanceCount = -1;

	private int instanceIndex = -1;

	private int maxAttempts = 3;

	private int backOffInitialInterval = 1000;

	private int backOffMaxInterval = 10000;

	private double backOffMultiplier = 2.0;

	private HeaderMode headerMode;

	private boolean useNativeDecoding;

	@Min(value = 1, message = "Concurrency should be greater than zero.")
	public int getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public boolean isPartitioned() {
		return partitioned;
	}

	public void setPartitioned(boolean partitioned) {
		this.partitioned = partitioned;
	}

	@Min(value = 1, message = "Instance count should be greater than zero.")
	public int getInstanceCount() {
		return instanceCount;
	}

	public void setInstanceCount(int instanceCount) {
		this.instanceCount = instanceCount;
	}

	@Min(value = 0, message = "Instance index should be greater than or equal to 0")
	public int getInstanceIndex() {
		return instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	@Min(value = 1, message = "Max attempts should be greater than zero.")
	public int getMaxAttempts() {
		return maxAttempts;
	}

	public void setMaxAttempts(int maxAttempts) {
		this.maxAttempts = maxAttempts;
	}

	@Min(value = 1, message = "Backoff initial interval should be greater than zero.")
	public int getBackOffInitialInterval() {
		return backOffInitialInterval;
	}

	public void setBackOffInitialInterval(int backOffInitialInterval) {
		this.backOffInitialInterval = backOffInitialInterval;
	}

	@Min(value = 1, message = "Backoff max interval should be greater than zero.")
	public int getBackOffMaxInterval() {
		return backOffMaxInterval;
	}

	public void setBackOffMaxInterval(int backOffMaxInterval) {
		this.backOffMaxInterval = backOffMaxInterval;
	}

	@Min(value = 1, message = "Backoff multiplier should be greater than zero.")
	public double getBackOffMultiplier() {
		return backOffMultiplier;
	}

	public void setBackOffMultiplier(double backOffMultiplier) {
		this.backOffMultiplier = backOffMultiplier;
	}

	public HeaderMode getHeaderMode() {
		return this.headerMode;
	}

	public void setHeaderMode(HeaderMode headerMode) {
		this.headerMode = headerMode;
	}

	public boolean isUseNativeDecoding() {
		return useNativeDecoding;
	}

	public void setUseNativeDecoding(boolean useNativeDecoding) {
		this.useNativeDecoding = useNativeDecoding;
	}
}
