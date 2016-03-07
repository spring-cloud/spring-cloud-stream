/*
 * Copyright 2015 the original author or authors.
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
import org.springframework.core.io.Resource;

/**
 * @author David Turanski
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.binder.rabbit")
class RabbitBinderConfigurationProperties {

	private String[] addresses = new String[0];

	private String[] adminAdresses = new String[0];

	private String[] nodes = new String[0];

	private String username;

	private String password;

	private String vhost;

	private boolean useSSL;

	private Resource sslPropertiesLocation;

	private int compressionLevel;

	public String[] getAddresses() {
		return addresses;
	}

	public void setAddresses(String[] addresses) {
		this.addresses = addresses;
	}

	public String[] getAdminAdresses() {
		return adminAdresses;
	}

	public void setAdminAdresses(String[] adminAdresses) {
		this.adminAdresses = adminAdresses;
	}

	public String[] getNodes() {
		return nodes;
	}

	public void setNodes(String[] nodes) {
		this.nodes = nodes;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getVhost() {
		return vhost;
	}

	public void setVhost(String vhost) {
		this.vhost = vhost;
	}

	public boolean isUseSSL() {
		return useSSL;
	}

	public void setUseSSL(boolean useSSL) {
		this.useSSL = useSSL;
	}

	public Resource getSslPropertiesLocation() {
		return sslPropertiesLocation;
	}

	public void setSslPropertiesLocation(Resource sslPropertiesLocation) {
		this.sslPropertiesLocation = sslPropertiesLocation;
	}

	public int getCompressionLevel() {
		return compressionLevel;
	}

	public void setCompressionLevel(int compressionLevel) {
		this.compressionLevel = compressionLevel;
	}
}
