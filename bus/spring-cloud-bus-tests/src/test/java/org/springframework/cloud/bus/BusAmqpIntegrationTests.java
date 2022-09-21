/*
 * Copyright 2015-2020 the original author or authors.
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT, properties = { "management.endpoints.web.exposure.include=*",
		"spring.cloud.stream.bindings.springCloudBusOutput.producer.errorChannelEnabled=true",
		"logging.level.org.springframework.cloud.bus=TRACE", "spring.cloud.bus.id=app:1",
		"spring.autoconfigure.exclude=org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration" })
@Testcontainers
public class BusAmqpIntegrationTests {

	@Container
	private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer(
			"rabbitmq:3.7.25-management-alpine");

	private static ConfigurableApplicationContext context;

	@Autowired
	private BindingServiceProperties bindingServiceProperties;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		registry.add("spring.rabbitmq.host", rabbitMQContainer::getHost);
		registry.add("spring.rabbitmq.port", rabbitMQContainer::getAmqpPort);
	}

	@BeforeAll
	static void before() {
		context = new SpringApplicationBuilder(TestConfig.class).properties("server.port=0",
				"spring.rabbitmq.host=" + rabbitMQContainer.getHost(),
				"spring.rabbitmq.port=" + rabbitMQContainer.getAmqpPort(),
				"management.endpoints.web.exposure.include=*", "spring.cloud.bus.id=app:2",
				"spring.autoconfigure.exclude=org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration")
				.run();
	}

	@AfterAll
	static void after() {
		if (context != null) {
			context.close();
		}
	}

	@Test
	void remoteEventsAreSentViaAmqp(@Autowired WebTestClient client, @Autowired TestConfig testConfig)
			throws InterruptedException {
		assertThat(rabbitMQContainer.isRunning());
		HashMap<String, String> map = new HashMap<>();
		map.put("name", "foo");
		map.put("value", "bar");
		client.post().uri("/actuator/busenv").bodyValue(map).exchange().expectStatus().is2xxSuccessful();
		TestConfig remoteTestConfig = context.getBean(TestConfig.class);
		assertThat(remoteTestConfig.latch.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(testConfig.latch.await(5, TimeUnit.SECONDS)).isTrue();
		ProducerProperties producerProperties = bindingServiceProperties.getProducerProperties(BusConstants.OUTPUT);
		assertThat(producerProperties.isErrorChannelEnabled()).isTrue();
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
