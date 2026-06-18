/*
 * Copyright 2024-present the original author or authors.
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

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.endpoint.SourcePollingChannelAdapter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for gh-3203: an output-only binding that has no backing
 * {@code Supplier}/{@code Function} bean must not be treated as a {@code Supplier}.
 * Prior to the fix the standalone output binding (whose function definition is the
 * binding name) reached {@code supplierInitializer}, which built a
 * {@link SourcePollingChannelAdapter} on top of the plain output channel and threw a
 * {@code ClassCastException} (the channel cannot be cast to {@code Supplier}) on the
 * first poll.
 *
 * @author Spring Cloud Stream contributors
 */
class OutputOnlyBindingSupplierTests {

	@Test
	void outputOnlyBindingIsNotTreatedAsSupplier() {
		try (ConfigurableApplicationContext context = new SpringApplication(EmptyConfiguration.class).run(
				"--spring.cloud.stream.output-bindings=testOutput",
				"--spring.jmx.enabled=false",
				"--spring.main.web-application-type=none",
				"--spring.cloud.stream.default-binder=mock")) {

			Map<String, SourcePollingChannelAdapter> pollingAdapters =
					context.getBeansOfType(SourcePollingChannelAdapter.class);

			assertThat(pollingAdapters)
					.as("No SourcePollingChannelAdapter must be created for an output-only "
							+ "binding without a backing Supplier")
					.isEmpty();
		}
	}

	@EnableAutoConfiguration
	@Configuration
	public static class EmptyConfiguration {

	}

}
