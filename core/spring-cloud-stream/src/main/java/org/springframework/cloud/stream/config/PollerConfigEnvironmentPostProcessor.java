/*
 * Copyright 2021-present the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 *
 * @author Artem Bilan
 *
 * @since 3.2
 */
public class PollerConfigEnvironmentPostProcessor implements EnvironmentPostProcessor {

	private static final Log logger = LogFactory.getLog(PollerConfigEnvironmentPostProcessor.class);

	private static final String STREAM_PROPERTY_PREFIX = "spring.cloud.stream.poller.";

	private static final String INTEGRATION_PROPERTY_PREFIX = "spring.integration.poller.";

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		Map<String, Object> streamPollerProperties = new HashMap<>();

		String[] pollerPropertySuffixes = { "fixed-delay", "max-messages-per-poll", "cron", "initial-delay" };

		PropertyMapper map = PropertyMapper.get().alwaysApplying(PropertyMapper.Source::whenHasText);

		for (String pollerPropertySuffix : pollerPropertySuffixes) {
			map.from(environment.getProperty(STREAM_PROPERTY_PREFIX + pollerPropertySuffix))
				.to((value) -> streamPollerProperties.put(INTEGRATION_PROPERTY_PREFIX + pollerPropertySuffix, value));
		}

		if (!streamPollerProperties.isEmpty()) {
			logger.info("'spring.cloud.stream.poller' " +
				"properties are deprecated in favor of 'spring.integration.poller' properties.");
		}

		//TODO Must remain after removal of deprecated code above in the future
		if (!streamPollerProperties.containsKey(INTEGRATION_PROPERTY_PREFIX + "cron") &&
			!environment.containsProperty(INTEGRATION_PROPERTY_PREFIX + "cron") &&
			!environment.containsProperty(INTEGRATION_PROPERTY_PREFIX + "fixed-rate") &&
			!environment.containsProperty(INTEGRATION_PROPERTY_PREFIX + "fixed-delay")) {

			streamPollerProperties.putIfAbsent(INTEGRATION_PROPERTY_PREFIX + "fixed-delay", "1s");
		}
		if (!environment.containsProperty(INTEGRATION_PROPERTY_PREFIX + "max-messages-per-poll")) {
			streamPollerProperties.putIfAbsent(INTEGRATION_PROPERTY_PREFIX + "max-messages-per-poll", "1");
		}

		if (!streamPollerProperties.isEmpty()) {
			environment.getPropertySources()
				.addLast(new MapPropertySource("spring.integration.poller", streamPollerProperties));
		}
	}

}
