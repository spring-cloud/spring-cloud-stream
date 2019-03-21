/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.schema.client.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.schema-registry-client")
public class SchemaRegistryClientProperties {

	private String endpoint;

	private boolean cached = false;

	public String getEndpoint() {
		return this.endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public boolean isCached() {
		return this.cached;
	}

	public void setCached(boolean cached) {
		this.cached = cached;
	}

}
