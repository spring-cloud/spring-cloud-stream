/*
 * Copyright 2013-2020 the original author or authors.
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

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Ryan Baxter
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
		classes = RefreshListenerIntegrationTests.MyApp.class,
		properties = { "management.endpoints.web.exposure.include=*", "spring.application.name=foobar" })
public class RefreshListenerIntegrationTests {

	@Autowired
	private TestRestTemplate rest;

	@MockBean
	private BusBridge busBridge;

	@Test
	public void testEndpoint() {
		System.out.println(rest.getForObject("/actuator", String.class));
		assertThat(rest.postForEntity("/actuator/busrefresh/demoapp", new HashMap<>(), String.class).getStatusCode())
				.isEqualTo(HttpStatus.NO_CONTENT);
		assertThat(rest.postForEntity("/actuator/busrefresh/foobar", new HashMap<>(), String.class).getStatusCode())
				.isEqualTo(HttpStatus.NO_CONTENT);
		verify(busBridge, times(2)).send(any());
	}

	@SpringBootApplication
	@Import(TestChannelBinderConfiguration.class)
	static class MyApp {

	}

}
