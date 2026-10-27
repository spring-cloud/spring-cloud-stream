/*
 * Copyright 2012-present the original author or authors.
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
import org.springframework.cloud.bus.endpoint.TraceBusEndpoint;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link TraceBusEndpoint}.
 *
 * @author Ngoc Nhan
 */
@SpringBootTest(
		properties = { "management.endpoints.web.exposure.include=bustrace", "spring.cloud.bus.trace.enabled=true" })
public class BusTraceEndpointTests {

	@Autowired(required = false)
	private TraceBusEndpoint traceBusEndpoint;

	@Test
	public void contextLoads() {
		assertThat(this.traceBusEndpoint).isNotNull();
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	protected static class TestConfig {

	}

}
