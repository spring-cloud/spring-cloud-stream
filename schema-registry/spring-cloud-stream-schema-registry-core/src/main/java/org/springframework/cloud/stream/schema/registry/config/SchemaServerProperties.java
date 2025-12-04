/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.cloud.stream.schema.registry.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Vinicius Carvalho
 * @author Ilayaperumal Gopinathan
 * @author Christian Tzolov
 */
@ConfigurationProperties("spring.cloud.stream.schema.server")
public class SchemaServerProperties {

	/**
	 * Prefix for configuration resource paths (default is empty). Useful when embedding
	 * in another application when you don't want to change the context path or servlet
	 * path.
	 */
	private String path;

	/**
	 * Boolean flag to enable/disable schema deletion.
	 */
	private boolean allowSchemaDeletion;

	public String getPath() {
		return this.path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public boolean isAllowSchemaDeletion() {
		return this.allowSchemaDeletion;
	}

	public void setAllowSchemaDeletion(boolean allowSchemaDeletion) {
		this.allowSchemaDeletion = allowSchemaDeletion;
	}

}
