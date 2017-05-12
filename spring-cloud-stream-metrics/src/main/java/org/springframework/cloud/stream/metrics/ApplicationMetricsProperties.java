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
import java.util.Map.Entry;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.boot.actuate.metrics.export.MetricExportProperties;
import org.springframework.boot.actuate.metrics.export.TriggerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.cloud.stream.metrics.config.BinderMetricsAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;

/**
 * @author Vinicius Carvalho
 * @author Janne Valkealahti
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.metrics")
public class ApplicationMetricsProperties
		implements EnvironmentAware, ApplicationContextAware {

	private static final Bindable<Map<String, String>> STRING_STRING_MAP = Bindable
			.mapOf(String.class, String.class);

	private final MetricExportProperties metricExportProperties;

	private String prefix = "";

	@Value("${spring.application.name:${vcap.application.name:${spring.config.name:application}}}")
	private String key;

	private String metricName;

	private String[] properties;

	private Environment environment;

	private ApplicationContext applicationContext;

	/**
	 * List of properties that are going to be appended to each message. This gets
	 * populate by onApplicationEvent, once the context refreshes to avoid overhead of
	 * doing per message basis.
	 */
	private Map<String, Object> exportProperties = null;

	public ApplicationMetricsProperties(MetricExportProperties metricExportProperties) {
		Assert.notNull(metricExportProperties, "'metricsExportProperties' cannot be null");
		this.metricExportProperties = metricExportProperties;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public TriggerProperties getTrigger() {
		return metricExportProperties
				.findTrigger(BinderMetricsAutoConfiguration.APPLICATION_METRICS_EXPORTER_TRIGGER_NAME);
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

	public String[] getProperties() {
		return properties;
	}

	public void setProperties(String[] properties) {
		this.properties = properties;
	}

	public Map<String, Object> getExportProperties() {
		if (this.exportProperties == null) {
			this.exportProperties = buildExportProperties();
		}
		return this.exportProperties;
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

	private boolean isMatch(String name, String[] includes, String[] excludes) {
		if (ObjectUtils.isEmpty(includes)
				|| PatternMatchUtils.simpleMatch(includes, name)) {
			return !PatternMatchUtils.simpleMatch(excludes, name);
		}
		return false;
	}

	private Map<String, Object> buildExportProperties() {
		Map<String, Object> props = new HashMap<>();
		if (!ObjectUtils.isEmpty(this.properties)) {
			Map<String, String> target = bindProperties();

			BeanExpressionResolver beanExpressionResolver = ((ConfigurableApplicationContext) applicationContext)
					.getBeanFactory().getBeanExpressionResolver();
			BeanExpressionContext expressionContext = new BeanExpressionContext(
					((ConfigurableApplicationContext) applicationContext).getBeanFactory(), null);
			for (Entry<String, String> entry : target.entrySet()) {
				if (isMatch(entry.getKey(), this.properties, null)) {
					String stringValue = ObjectUtils.nullSafeToString(entry.getValue());
					Object exportedValue = null;
					if (stringValue != null) {
						exportedValue = stringValue.startsWith("#{")
								? beanExpressionResolver.evaluate(
										environment.resolvePlaceholders(stringValue), expressionContext)
								: environment.resolvePlaceholders(stringValue);
					}

					props.put(entry.getKey(), exportedValue);
				}
			}
		}
		return props;
	}

	private Map<String, String> bindProperties() {
		Map<String, String> target;
		BindResult<Map<String, String>> bindResult = Binder.get(environment).bind("", STRING_STRING_MAP);
		if (bindResult.isBound()) {
			target = bindResult.get();
		}
		else {
			target = new HashMap<>();
		}
		return target;
	}
}
