/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author David Turanski
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.rabbit.binder")
class RabbitBinderConfigurationProperties {

	private String[] adminAddresses = new String[0];

	private String[] nodes = new String[0];

	private int compressionLevel;

	public String[] getAdminAddresses() {
		return adminAddresses;
	}

	public void setAdminAddresses(String[] adminAddresses) {
		this.adminAddresses = adminAddresses;
	}

	/**
	 * @param adminAddresses A comma-separated list of RabbitMQ management plugin URLs.
	 * @deprecated in favor of {@link #setAdminAddresses(String[])}. Will be removed in a
	 * future release.
	 */
	@Deprecated
	public void setAdminAdresses(String[] adminAddresses) {
		setAdminAddresses(adminAddresses);
	}

	public String[] getNodes() {
		return nodes;
	}

	public void setNodes(String[] nodes) {
		this.nodes = nodes;
	}

	public int getCompressionLevel() {
		return compressionLevel;
	}

	public void setCompressionLevel(int compressionLevel) {
		this.compressionLevel = compressionLevel;
	}
}
