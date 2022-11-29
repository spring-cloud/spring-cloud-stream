/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * An {@link EnvironmentPostProcessor} that sets some common configuration properties (log
 * config etc.,) for binder tests.
 *
 * @author Ilayaperumal Gopinathan
 */
public class BinderTestEnvironmentPostProcessor implements EnvironmentPostProcessor {

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment,
			SpringApplication application) {
		Map<String, Object> propertiesToAdd = new HashMap<>();
		propertiesToAdd.put("logging.pattern.console",
				"%d{ISO8601} %5p %t %c{2}:%L - %m%n");
		environment.getPropertySources().addLast(
				new MapPropertySource("binderTestPropertiesConfig", propertiesToAdd));
	}

}
