/*
 * Copyright 2015-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.rabbit.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author David Turanski
 * @author Gary Russell
 * @author Ben Blinebury
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.rabbit.binder")
public class RabbitBinderConfigurationProperties {

	/**
	 * Urls for management plugins; only needed for queue affinity.
	 */
	private String[] adminAddresses = new String[0];

	/**
	 * Cluster member node names; only needed for queue affinity.
	 */
	private String[] nodes = new String[0];

	/**
	 * Compression level for compressed bindings; see 'java.util.zip.Deflator'.
	 */
	private Integer compressionLevel;

	/**
	 * Prefix for connection names from this binder.
	 */
	private String connectionNamePrefix;

	public String[] getAdminAddresses() {
		return adminAddresses;
	}

	public void setAdminAddresses(String[] adminAddresses) {
		this.adminAddresses = adminAddresses;
	}

	public String[] getNodes() {
		return nodes;
	}

	public void setNodes(String[] nodes) {
		this.nodes = nodes;
	}

	public Integer getCompressionLevel() {
		return compressionLevel;
	}

	public void setCompressionLevel(Integer compressionLevel) {
		this.compressionLevel = compressionLevel;
	}

	public String getConnectionNamePrefix() {
		return this.connectionNamePrefix;
	}

	public void setConnectionNamePrefix(String connectionNamePrefix) {
		this.connectionNamePrefix = connectionNamePrefix;
	}

}
