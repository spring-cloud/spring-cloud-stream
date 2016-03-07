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

/**
 * Common consumer properties.
 *
 * @author Marius Bogoevici
 */
public class ConsumerProperties {

	private int concurrency = 1;

	private boolean partitioned = false;

	private int instanceCount = 1;

	private int instanceIndex = 0;

	private int maxAttempts = 3;

	private int backOffInitialInterval = 1000;

	private int backOffMaxInterval = 10000;

	private double backOffMultiplier = 2.0;

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

	public int getInstanceCount() {
		return instanceCount;
	}

	public void setInstanceCount(int instanceCount) {
		this.instanceCount = instanceCount;
	}

	public int getInstanceIndex() {
		return instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	public void setMaxAttempts(int maxAttempts) {
		this.maxAttempts = maxAttempts;
	}

	public int getMaxAttempts() {
		return maxAttempts;
	}

	public void setBackOffInitialInterval(int backOffInitialInterval) {
		this.backOffInitialInterval = backOffInitialInterval;
	}

	public int getBackOffInitialInterval() {
		return backOffInitialInterval;
	}

	public void setBackOffMaxInterval(int backOffMaxInterval) {
		this.backOffMaxInterval = backOffMaxInterval;
	}

	public int getBackOffMaxInterval() {
		return backOffMaxInterval;
	}

	public void setBackOffMultiplier(double backOffMultiplier) {
		this.backOffMultiplier = backOffMultiplier;
	}

	public double getBackOffMultiplier() {
		return backOffMultiplier;
	}

}

