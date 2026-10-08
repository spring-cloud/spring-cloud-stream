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

import java.util.List;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.cloud.bus.trace.Trace;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

/**
 * Test for {@link org.springframework.cloud.bus.endpoint.TraceBusEndpoint}.
 *
 * @author Ngoc Nhan
 */
@Testcontainers
@SpringBootTest(webEnvironment = RANDOM_PORT,
		properties = { "management.endpoints.web.exposure.include=busrefresh,bustrace",
				"spring.cloud.bus.trace.enabled=true" })
@AutoConfigureWebTestClient
public class TraceListenerIntegrationTests {

	@Container
	@ServiceConnection
	static RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq:4.0-management");

	@Test
	void busTrace(@Autowired WebTestClient client) {

		client.post().uri("/actuator/busrefresh").exchange().expectStatus().is2xxSuccessful();
		BusTraceResponse response = client.get()
			.uri("/actuator/bustrace")
			.exchange()
			.expectStatus()
			.is2xxSuccessful()
			.expectBody(BusTraceResponse.class)
			.returnResult()
			.getResponseBody();
		assertThat(response).isNotNull();
		assertThat(response.traces()).isNotEmpty().hasSize(2);
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	static class TestConfig {

	}

	record BusTraceResponse(List<Trace> traces) {

	}

}
