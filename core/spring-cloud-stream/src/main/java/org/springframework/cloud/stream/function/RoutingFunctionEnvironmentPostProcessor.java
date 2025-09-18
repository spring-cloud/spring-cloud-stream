/*
 * Copyright 2019-present the original author or authors.
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


package org.springframework.cloud.stream.function;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.cloud.function.context.config.RoutingFunction;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.StringUtils;
/**
 *
 * @author Oleg Zhurakousky
 * @since 2.2.1
 */
class RoutingFunctionEnvironmentPostProcessor implements EnvironmentPostProcessor {

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		String name = environment.getProperty("spring.cloud.function.definition");
		if (StringUtils.hasText(name) && (
				name.equals(RoutingFunction.FUNCTION_NAME) ||
				name.contains(RoutingFunction.FUNCTION_NAME + "|") ||
				name.contains("|" + RoutingFunction.FUNCTION_NAME)
			)) {
			((StandardEnvironment) environment).getSystemProperties()
				.putIfAbsent("spring.cloud.function.routing.enabled", "true");
		}
		// perhaps mpve it to separate class as it doesn't have much to do with RoutingFunction
		((StandardEnvironment) environment).getSystemProperties()
			.putIfAbsent("spring.cloud.function.configuration.default.copy-input-headers", "true");
	}

}
