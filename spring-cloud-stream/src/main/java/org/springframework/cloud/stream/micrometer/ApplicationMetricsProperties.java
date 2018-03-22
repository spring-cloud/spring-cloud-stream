/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.micrometer;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;

/**
 * @author Vinicius Carvalho
 * @author Janne Valkealahti
 * @author Oleg Zhurakousky
 */
@ConfigurationProperties(prefix = ApplicationMetricsProperties.PREFIX)
public class ApplicationMetricsProperties implements EnvironmentAware, ApplicationContextAware {

	public static final String PREFIX = "spring.cloud.stream.metrics";

	private static final Bindable<Map<String, String>> STRING_STRING_MAP = Bindable.mapOf(String.class, String.class);

	 /**
	 * The name of the metric being emitted. Should be an unique value per application.
	 * Defaults to: ${spring.application.name:${vcap.application.name:${spring.config.name:application}}}
	 */
	@Value("${spring.application.name:${vcap.application.name:${spring.config.name:application}}}")
	private String key;

	/**
	 * Application properties that should be added to the metrics payload
	 * For example: `spring.application**`
	 */
	private String[] properties;

	/**
	 * Interval expressed as Duration for scheduling metrics snapshots publishing.
	 * Defaults to PT60S (60 sec)
	 */
	private String scheduleInterval;

	/**
	 * List of properties that are going to be appended to each message. This gets
	 * populate by onApplicationEvent, once the context refreshes to avoid overhead of
	 * doing per message basis.
	 */
	private Map<String, Object> exportProperties;

	private Environment environment;

	private ApplicationContext applicationContext;

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
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

	public String getScheduleInterval() {
		return scheduleInterval;
	}

	public void setScheduleInterval(String scheduleInterval) {
		this.scheduleInterval = scheduleInterval;
	}

	private boolean isMatch(String name, String[] includes) {
		return ObjectUtils.isEmpty(includes) || PatternMatchUtils.simpleMatch(includes, name);
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
				if (isMatch(entry.getKey(), this.properties)) {
					String stringValue = ObjectUtils.nullSafeToString(entry.getValue());
					Object exportedValue = stringValue.startsWith("#{")
							? beanExpressionResolver.evaluate(
									environment.resolvePlaceholders(stringValue), expressionContext)
							: environment.resolvePlaceholders(stringValue);

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
