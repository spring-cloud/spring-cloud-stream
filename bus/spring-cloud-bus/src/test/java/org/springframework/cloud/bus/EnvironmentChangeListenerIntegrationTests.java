/*
 * Copyright 2026-present the original author or authors.
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

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
		classes = EnvironmentChangeListenerIntegrationTests.MyApp.class,
		properties = { "management.endpoints.web.exposure.include=*", "spring.application.name=foobar" })
@AutoConfigureTestRestTemplate
public class EnvironmentChangeListenerIntegrationTests {

	@Autowired
	private TestRestTemplate rest;

	@Autowired
	private Environment environment;

	@MockitoBean
	private BusBridge busBridge;

	@Test
	public void environmentIsOnlyChangedWhenEventTargetsApp() {
		assertThat(rest.postForEntity("/actuator/busenv/demoapp", body("bus.test.other", "changed"), String.class)
			.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
		assertThat(environment.getProperty("bus.test.other")).isNull();

		assertThat(rest.postForEntity("/actuator/busenv/foobar", body("bus.test.self", "changed"), String.class)
			.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
		assertThat(environment.getProperty("bus.test.self")).isEqualTo("changed");

		verify(busBridge, times(2)).send(any());
	}

	private static Map<String, String> body(String name, String value) {
		Map<String, String> body = new HashMap<>();
		body.put("name", name);
		body.put("value", value);
		return body;
	}

	// no component scan: it would pick up the bus configuration classes in this
	// package as regular configuration and evaluate them before auto-configuration
	@SpringBootConfiguration
	@EnableAutoConfiguration
	@Import(TestChannelBinderConfiguration.class)
	static class MyApp {

	}

}
