/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.config.metrics;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * @author Vinicius Carvalho
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.metrics")
public class StreamMetricsProperties {

	private String prefix;

	@Value("${spring.application.name:${vcap.application.name:${spring.config.name:application}}}")
	private String key;

	private String metricName;

	private String[] includes = new String[] { "integration**" };

	private String[] excludes;

	private String[] properties;

	private Long delayMillis;

	public String[] getIncludes() {
		return includes;
	}

	public void setIncludes(String[] includes) {
		this.includes = includes;
	}

	public String[] getExcludes() {
		return excludes;
	}

	public void setExcludes(String[] excludes) {
		this.excludes = excludes;
	}

	public Long getDelayMillis() {
		return delayMillis;
	}

	public void setDelayMillis(Long delayMillis) {
		this.delayMillis = delayMillis;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String[] getProperties() {
		return properties;
	}

	public void setProperties(String[] properties) {
		this.properties = properties;
	}

	public String getMetricName() {
		if (this.metricName == null) {
			this.metricName = resolveMetricName();
		}
		return metricName;
	}

	private String resolveMetricName() {
		StringBuffer name = new StringBuffer(this.key);
		if (!StringUtils.isEmpty(this.prefix)) {
			String prefix = this.prefix;
			if (prefix.lastIndexOf(".") == -1) {
				prefix += ".";
			}
			name.insert(0, prefix);
		}
		return name.toString();
	}
}
