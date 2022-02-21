/*
 * Copyright 2012-2022 the original author or authors.
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

package org.springframework.cloud.bus;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.cloud.commons.util.IdUtils;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.util.StringUtils;

import static org.springframework.cloud.bus.BusProperties.PREFIX;

/**
 * {@link EnvironmentPostProcessor} that sets the default properties for the Bus.
 *
 * @author Dave Syer
 * @since 1.0.0
 */
public class BusEnvironmentPostProcessor implements EnvironmentPostProcessor {

	static final String DEFAULTS_PROPERTY_SOURCE_NAME = "springCloudBusDefaultProperties";

	static final String OVERRIDES_PROPERTY_SOURCE_NAME = "springCloudBusOverridesProperties";

	private static final String FN_DEF_PROP = FunctionProperties.PREFIX + ".definition";

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		if (environment.containsProperty(ConditionalOnBusEnabled.SPRING_CLOUD_BUS_ENABLED)) {
			if (Boolean.FALSE.toString()
					.equalsIgnoreCase(environment.getProperty(ConditionalOnBusEnabled.SPRING_CLOUD_BUS_ENABLED))) {
				return;
			}
		}
		Map<String, Object> overrides = new HashMap<>();
		String definition = BusConstants.BUS_CONSUMER;
		if (environment.containsProperty(FN_DEF_PROP)) {
			String property = environment.getProperty(FN_DEF_PROP);
			if (property != null && property.contains(BusConstants.BUS_CONSUMER)) {
				// in the case that EnvironmentPostProcessor are run more than once.
				return;
			}
			definition = property + ";" + definition;
		}
		overrides.put(FN_DEF_PROP, definition);
		addOrReplace(environment.getPropertySources(), overrides, OVERRIDES_PROPERTY_SOURCE_NAME, true);

		Map<String, Object> defaults = new HashMap<>();
		defaults.put("spring.cloud.stream.function.bindings." + BusConstants.BUS_CONSUMER + "-in-0",
				BusConstants.INPUT);
		String destination = environment.getProperty(PREFIX + ".destination", BusConstants.DESTINATION);
		defaults.put("spring.cloud.stream.bindings." + BusConstants.INPUT + ".destination", destination);
		defaults.put("spring.cloud.stream.bindings." + BusConstants.OUTPUT + ".destination", destination);
		if (!environment.containsProperty(PREFIX + ".id")) {
			String unresolvedServiceId = IdUtils.getUnresolvedServiceId();
			if (StringUtils.hasText(environment.getProperty("spring.profiles.active"))) {
				unresolvedServiceId = IdUtils.getUnresolvedServiceIdWithActiveProfiles();
			}
			defaults.put(PREFIX + ".id", unresolvedServiceId);
		}
		addOrReplace(environment.getPropertySources(), defaults, DEFAULTS_PROPERTY_SOURCE_NAME, false);
	}

	public static void addOrReplace(MutablePropertySources propertySources, Map<String, Object> map,
			String propertySourceName, boolean first) {
		MapPropertySource target = null;
		if (propertySources.contains(propertySourceName)) {
			PropertySource<?> source = propertySources.get(propertySourceName);
			if (source instanceof MapPropertySource) {
				target = (MapPropertySource) source;
				for (String key : map.keySet()) {
					if (!target.containsProperty(key)) {
						target.getSource().put(key, map.get(key));
					}
				}
			}
		}
		if (target == null) {
			target = new MapPropertySource(propertySourceName, map);
		}
		if (!propertySources.contains(propertySourceName)) {
			if (first) {
				propertySources.addFirst(target);
			}
			else {
				propertySources.addLast(target);
			}
		}
	}

}
