/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.integration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import brave.handler.SpanHandler;
import brave.test.TestSpanHandler;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.brave.bridge.BraveFinishedSpan;
import io.micrometer.tracing.test.simple.SpansAssert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.testcontainers.containers.RabbitMQContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.rabbit.RabbitTestContainer;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Artem Bilan
 * @since 4.1.1
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
		args = "--spring.config.location=classpath:/rabbit-multi-binder-observation.yml")
@DirtiesContext
//@AutoConfigureMetric
@AutoConfigureTracing
@Disabled
public class RabbitMultiBinderObservationTests {

	private static final TestSpanHandler SPANS = new TestSpanHandler();

	private static final RabbitMQContainer RABBITMQ = RabbitTestContainer.sharedInstance();

	@Autowired
	StreamBridge streamBridge;

	@Autowired
	ObservationRegistry observationRegistry;

	@Autowired
	TestConfiguration testConfiguration;

	@DynamicPropertySource
	static void rabbitProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.rabbitmq.port", RABBITMQ::getAmqpPort);
	}

	@Test
	void observationIsPropagatedInMultiBinderConfiguration() throws InterruptedException {
		Observation.createNotStarted("test parent observation", this.observationRegistry)
			.observe(() -> this.streamBridge.send("test-out-0", "test data"));

		assertThat(this.testConfiguration.messageReceived.await(10, TimeUnit.SECONDS)).isTrue();

		// There is a race condition when we already have a reply, but the span in the
		// Rabbit listener is not closed yet.
		// parent -> StreamBridge -> RabbitTemplate -> Rabbit Listener -> Consumer
		await().untilAsserted(() -> assertThat(SPANS.spans()).hasSize(6));
		SpansAssert.assertThat(SPANS.spans().stream().map(BraveFinishedSpan::fromBrave).collect(Collectors.toList()))
			.haveSameTraceId();
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	public static class TestConfiguration {

		final CountDownLatch messageReceived = new CountDownLatch(1);

		@Bean
		SpanHandler testSpanHandler() {
			return SPANS;
		}

		@Bean
		public Consumer<Message<?>> testListener() {
			return message -> this.messageReceived.countDown();
		}

	}

}
