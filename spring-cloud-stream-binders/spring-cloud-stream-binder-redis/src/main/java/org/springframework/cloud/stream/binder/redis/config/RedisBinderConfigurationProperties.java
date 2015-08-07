/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.binder.redis.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author David Turanski
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.binder.redis.default")
class RedisBinderConfigurationProperties {

	private int backOffInitialInterval;
	private int backOffMaxInterval;
	private double backOffMultiplier;
	private int concurrency;
	private int maxAttempts;

	public int getBackOffInitialInterval() {
		return backOffInitialInterval;
	}

	public void setBackOffInitialInterval(int backOffInitialInterval) {
		this.backOffInitialInterval = backOffInitialInterval;
	}

	public int getBackOffMaxInterval() {
		return backOffMaxInterval;
	}

	public void setBackOffMaxInterval(int backOffMaxInterval) {
		this.backOffMaxInterval = backOffMaxInterval;
	}

	public double getBackOffMultiplier() {
		return backOffMultiplier;
	}

	public void setBackOffMultiplier(double backOffMultiplier) {
		this.backOffMultiplier = backOffMultiplier;
	}

	public int getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public int getMaxAttempts() {
		return maxAttempts;
	}

	public void setMaxAttempts(int maxAttempts) {
		this.maxAttempts = maxAttempts;
	}
}
