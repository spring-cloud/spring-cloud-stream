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

package org.springframework.cloud.stream.binder.local.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author David Turanski
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.binder.local.executor")
class LocalExecutorConfigurationProperties {
	
	private int executorCorePoolSize;
	
	private int executorMaxPoolSize;
	
	private int executorQueueSize = Integer.MAX_VALUE;
	
	private int executorKeepAliveSeconds;

	public int getExecutorCorePoolSize() {
		return executorCorePoolSize;
	}

	public void setExecutorCorePoolSize(int executorCorePoolSize) {
		this.executorCorePoolSize = executorCorePoolSize;
	}

	public int getExecutorMaxPoolSize() {
		return executorMaxPoolSize;
	}

	public void setExecutorMaxPoolSize(int executorMaxPoolSize) {
		this.executorMaxPoolSize = executorMaxPoolSize;
	}

	public int getExecutorQueueSize() {
		return executorQueueSize;
	}

	public void setExecutorQueueSize(int executorQueueSize) {
		this.executorQueueSize = executorQueueSize;
	}

	public int getExecutorKeepAliveSeconds() {
		return executorKeepAliveSeconds;
	}

	public void setExecutorKeepAliveSeconds(int executorKeepAliveSeconds) {
		this.executorKeepAliveSeconds = executorKeepAliveSeconds;
	}
}
