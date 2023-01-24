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

package org.springframework.cloud.bus;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.bus.endpoint.EnvironmentBusEndpoint;
import org.springframework.cloud.bus.endpoint.RefreshBusEndpoint;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = { "spring.jmx.enabled=true", "endpoints.default.jmx.enabled=true",
		"management.endpoints..jmx.exposure.include=busrefresh,busenv", "debug=true" })
public class BusJmxEndpointTests {

	@Autowired(required = false)
	private RefreshBusEndpoint refreshBusEndpoint;

	@Autowired(required = false)
	private EnvironmentBusEndpoint environmentBusEndpoint;

	@Test
	public void contextLoads() {
		assertThat(this.refreshBusEndpoint).isNotNull();
		assertThat(this.environmentBusEndpoint).isNotNull();
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	protected static class TestConfig {

	}

}
