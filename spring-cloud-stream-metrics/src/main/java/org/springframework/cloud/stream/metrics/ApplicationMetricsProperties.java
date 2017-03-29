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

package org.springframework.cloud.stream.metrics;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.bind.RelaxedNames;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;

/**
 * @author Vinicius Carvalho
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.metrics")
public class ApplicationMetricsProperties
		implements ApplicationListener<ContextRefreshedEvent> {

	private String prefix = "";

	@Value("${spring.application.name:${vcap.application.name:${spring.config.name:application}}}")
	private String key;

	@Value("${INSTANCE_INDEX:${CF_INSTANCE_INDEX:0}}")
	private int instanceIndex;

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
		if (!prefix.endsWith(".")) {
			prefix += ".";
		}
		this.prefix = prefix;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getInstanceIndex() {
		return instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	public String[] getProperties() {
		return properties;
	}

	public void setProperties(String[] properties) {
		this.properties = properties;
	}

	/**
	 * List of properties that are going to be appended to each message. This gets
	 * populate by onApplicationEvent, once the context refreshes to avoid overhead of
	 * doing per message basis.
	 */
	private Map<String, Object> exportProperties = new HashMap<>();

	public Map<String, Object> getExportProperties() {
		return exportProperties;
	}

	public String getMetricName() {
		if (this.metricName == null) {
			this.metricName = resolveMetricName();
		}
		return metricName;
	}

	private String resolveMetricName() {
		return this.prefix + this.key;
	}

	/**
	 * Iterates over all property sources from this application context and copies the
	 * ones listed in {@link ApplicationMetricsProperties} includes.
	 */
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		ConfigurableApplicationContext ctx = (ConfigurableApplicationContext) event
				.getSource();
		if (!ObjectUtils.isEmpty(this.properties)) {
			for (PropertySource<?> source : ctx.getEnvironment().getPropertySources()) {
				if (source instanceof EnumerablePropertySource) {
					EnumerablePropertySource<?> e = (EnumerablePropertySource<?>) source;
					for (String propertyName : e.getPropertyNames()) {
						RelaxedNames relaxedNames = new RelaxedNames(propertyName);
						relaxedLoop: for (String relaxedPropertyName : relaxedNames) {
							if (isMatch(relaxedPropertyName, this.properties, null)) {
								this.exportProperties.put(
										RelaxedPropertiesUtils
												.findCanonicalFormat(relaxedNames),
										source.getProperty(propertyName));
								break relaxedLoop;
							}
						}
					}
				}
			}
		}
	}

	private boolean isMatch(String name, String[] includes, String[] excludes) {
		if (ObjectUtils.isEmpty(includes)
				|| PatternMatchUtils.simpleMatch(includes, name)) {
			return !PatternMatchUtils.simpleMatch(excludes, name);
		}
		return false;
	}

}
