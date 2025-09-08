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

import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

/**
 * @author Ryan Baxter
 */
@SpringBootTest(webEnvironment = RANDOM_PORT,
		properties = { "management.endpoints.web.exposure.include=*",
				"spring.cloud.stream.bindings.springCloudBusOutput.producer.errorChannelEnabled=true",
				"logging.level.org.springframework.cloud.bus=TRACE", "spring.cloud.bus.id=app:1" })
@Testcontainers
public class ShutdownListenerIntegrationTests {

	private static ConfigurableApplicationContext context;

	@Container
	@ServiceConnection
	private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq:4.0-management");

	@BeforeAll
	static void before() {
		context = new SpringApplicationBuilder(TestConfig.class)
			.properties("server.port=0", "spring.rabbitmq.host=" + rabbitMQContainer.getHost(),
					"spring.rabbitmq.port=" + rabbitMQContainer.getAmqpPort(),
					"management.endpoints.web.exposure.include=*", "spring.cloud.bus.id=app:2", "debug=true")
			.run();
	}

	@AfterAll
	static void after() {
		if (context != null) {
			context.close();
		}
	}

	@Test
	void testShutdown(@Autowired WebTestClient client) {
		assertThat(rabbitMQContainer.isRunning());
		client.post().uri("/actuator/busshutdown/app:2").exchange().expectStatus().is2xxSuccessful();
		assertThat(context.isClosed());
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	static class TestConfig implements ApplicationListener<EnvironmentChangeRemoteApplicationEvent> {

		CountDownLatch latch = new CountDownLatch(1);

		@Override
		public void onApplicationEvent(EnvironmentChangeRemoteApplicationEvent event) {
			latch.countDown();
		}

	}

}
