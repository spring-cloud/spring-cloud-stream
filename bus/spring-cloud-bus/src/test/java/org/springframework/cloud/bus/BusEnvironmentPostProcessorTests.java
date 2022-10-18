/*
 * Copyright 2015-2022 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.springframework.cloud.bus.BusConstants.BUS_CONSUMER;
import static org.springframework.cloud.bus.BusConstants.DESTINATION;
import static org.springframework.cloud.bus.BusConstants.INPUT;
import static org.springframework.cloud.bus.BusConstants.OUTPUT;
import static org.springframework.cloud.bus.BusEnvironmentPostProcessor.DEFAULTS_PROPERTY_SOURCE_NAME;
import static org.springframework.cloud.bus.BusEnvironmentPostProcessor.OVERRIDES_PROPERTY_SOURCE_NAME;

public class BusEnvironmentPostProcessorTests {

	@Test
	void testDefaults() {
		MockEnvironment env = new MockEnvironment().withProperty("cachedrandom.application.value", "123");
		new BusEnvironmentPostProcessor().postProcessEnvironment(env, mock(SpringApplication.class));
		assertThat(env.getProperty(FunctionProperties.PREFIX + ".definition")).isEqualTo(BUS_CONSUMER);
		assertThat(env.getProperty("spring.cloud.stream.function.bindings." + BUS_CONSUMER + "-in-0")).isEqualTo(INPUT);
		assertThat(env.getProperty("spring.cloud.stream.bindings." + INPUT + ".destination")).isEqualTo(DESTINATION);
		assertThat(env.getProperty("spring.cloud.stream.bindings." + OUTPUT + ".destination")).isEqualTo(DESTINATION);
		assertThat(env.getProperty(BusProperties.PREFIX + ".id")).isEqualTo("application:8080:123");
		assertThat(env.getPropertySources().contains(OVERRIDES_PROPERTY_SOURCE_NAME));
		assertThat(env.getPropertySources().contains(DEFAULTS_PROPERTY_SOURCE_NAME));
	}

	@Test
	void testWithActiveProfile() {
		MockEnvironment env = new MockEnvironment().withProperty("cachedrandom.application.value", "123")
				.withProperty("spring.profiles.active", "dev");
		new BusEnvironmentPostProcessor().postProcessEnvironment(env, mock(SpringApplication.class));
		assertThat(env.getProperty(FunctionProperties.PREFIX + ".definition")).isEqualTo(BUS_CONSUMER);
		assertThat(env.getProperty("spring.cloud.stream.function.bindings." + BUS_CONSUMER + "-in-0")).isEqualTo(INPUT);
		assertThat(env.getProperty("spring.cloud.stream.bindings." + INPUT + ".destination")).isEqualTo(DESTINATION);
		assertThat(env.getProperty("spring.cloud.stream.bindings." + OUTPUT + ".destination")).isEqualTo(DESTINATION);
		assertThat(env.getProperty(BusProperties.PREFIX + ".id")).isEqualTo("application:dev:8080:123");
		assertThat(env.getPropertySources().contains(OVERRIDES_PROPERTY_SOURCE_NAME));
		assertThat(env.getPropertySources().contains(DEFAULTS_PROPERTY_SOURCE_NAME));
	}

	@Test
	void testOverrides() {
		String fnDefKey = FunctionProperties.PREFIX + ".definition";
		String idKey = BusProperties.PREFIX + ".id";
		MockEnvironment env = new MockEnvironment().withProperty("cachedrandom.application.value", "123")
				.withProperty(BusProperties.PREFIX + ".destination", "mydestination").withProperty(idKey, "app:1")
				.withProperty(fnDefKey, "uppercase");
		new BusEnvironmentPostProcessor().postProcessEnvironment(env, mock(SpringApplication.class));
		assertThat(env.getProperty(fnDefKey)).isEqualTo("uppercase;" + BUS_CONSUMER);
		assertThat(env.getProperty("spring.cloud.stream.function.bindings." + BUS_CONSUMER + "-in-0")).isEqualTo(INPUT);
		assertThat(env.getProperty("spring.cloud.stream.bindings." + INPUT + ".destination"))
				.isEqualTo("mydestination");
		assertThat(env.getProperty("spring.cloud.stream.bindings." + OUTPUT + ".destination"))
				.isEqualTo("mydestination");
		assertThat(env.getProperty(idKey)).isEqualTo("app:1");
		assertThat(env.getPropertySources().contains(OVERRIDES_PROPERTY_SOURCE_NAME));
		assertThat(env.getPropertySources().contains(DEFAULTS_PROPERTY_SOURCE_NAME));
	}

}
