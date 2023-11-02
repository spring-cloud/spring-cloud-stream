/*
 * Copyright 2021-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.test;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.ReflectionUtils;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
class TestChannelBinderTests {

	@Test
	public void test() throws Exception {
		// nothing to assert. no failure on this test signifies success
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SampleConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.function-in-0.destination=input")) {
			TestChannelBinder binder = context.getBean(TestChannelBinder.class);
			Method registerErrorInfrastructure = ReflectionUtils
				.findMethod(TestChannelBinder.class, "registerErrorInfrastructure", ProducerDestination.class, String.class);
			registerErrorInfrastructure.setAccessible(true);
			ProducerDestination destination = new ProducerDestination() {
				@Override
				public String getNameForPartition(int partition) {
					return "sample";
				}

				@Override
				public String getName() {
					return "sample";
				}
			};
			registerErrorInfrastructure.invoke(binder, destination, "function-in-0");
			destination = new ProducerDestination() {
				@Override
				public String getNameForPartition(int partition) {
					return "sample";
				}

				@Override
				public String getName() {
					return "sample";
				}
			};
			registerErrorInfrastructure.invoke(binder, destination, "function-in-0");
		}
	}

	@EnableAutoConfiguration
	public static class SampleConfiguration {

	}
}
