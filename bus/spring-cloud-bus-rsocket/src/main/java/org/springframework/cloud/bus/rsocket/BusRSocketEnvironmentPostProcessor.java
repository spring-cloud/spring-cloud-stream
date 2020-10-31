/*
 * Copyright 2012-2019 the original author or authors.
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

package org.springframework.cloud.bus.rsocket;

import java.util.HashMap;
import java.util.Map;

import io.rsocket.routing.client.spring.RoutingClientProperties;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;

import static org.springframework.cloud.bus.BusEnvironmentPostProcessor.addOrReplace;

/**
 * {@link EnvironmentPostProcessor} that sets the default properties for the RSocket
 * Routing Client.
 */
public class BusRSocketEnvironmentPostProcessor implements EnvironmentPostProcessor {

	static final String DEFAULTS_PROPERTY_SOURCE_NAME = "springCloudBusRSocketDefaultProperties";

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		Map<String, Object> defaults = new HashMap<>();
		defaults.put(RoutingClientProperties.CONFIG_PREFIX + ".tags.bus", true);
		addOrReplace(environment.getPropertySources(), defaults, DEFAULTS_PROPERTY_SOURCE_NAME, false);
	}

}
