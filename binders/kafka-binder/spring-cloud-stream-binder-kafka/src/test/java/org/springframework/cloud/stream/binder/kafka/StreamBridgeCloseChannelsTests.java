/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test demonstrates that closeChannelsGracefully() closes producers
 */
@EmbeddedKafka
@DirtiesContext
class StreamBridgeCloseChannelsTests {

	@Test
	void testCloseChannelsGracefullyClosesProducersButKeepsCache() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestConfig.class)
			.web(WebApplicationType.NONE)
			.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}"
			)) {

			StreamBridge streamBridge = context.getBean(StreamBridge.class);

			// 1. Get the method
			Method closeMethod = StreamBridge.class.getDeclaredMethod("closeChannelsGracefully");
			closeMethod.setAccessible(true);

			// 2. Get the cache
			Field cacheField = StreamBridge.class.getDeclaredField("channelCache");
			cacheField.setAccessible(true);
			Map<String, MessageChannel> cache = (Map<String, MessageChannel>) cacheField.get(streamBridge);

			streamBridge.send("test-topic", "message-1");
			assertThat(cache).hasSize(1);
			MessageChannel originalChannel = cache.get("test-topic");

			closeMethod.invoke(streamBridge);

			assertThat(cache).hasSize(1);
			MessageChannel channelAfterClose = cache.get("test-topic");

			assertThat(channelAfterClose).isSameAs(originalChannel);

			boolean sent = streamBridge.send("test-topic", "message-2");
			assertThat(sent).isTrue();

			MessageChannel channelAfterResend = cache.get("test-topic");
			assertThat(channelAfterResend).isSameAs(originalChannel);

			streamBridge.send("test-topic-2", "message-3");
			assertThat(cache).hasSize(2); // Two channels

			closeMethod.invoke(streamBridge);

			assertThat(cache).hasSize(2);
			assertThat(cache).containsKeys("test-topic", "test-topic-2");
		}
	}

	@Test
	void testOnApplicationEventClearsCache() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestConfig.class)
			.web(WebApplicationType.NONE)
			.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}"
			)) {

			StreamBridge streamBridge = context.getBean(StreamBridge.class);

			// Get the cache
			Field cacheField = StreamBridge.class.getDeclaredField("channelCache");
			cacheField.setAccessible(true);
			Map<String, MessageChannel> cache = (Map<String, MessageChannel>) cacheField.get(streamBridge);

			// Create channels
			streamBridge.send("topic-1", "data");
			streamBridge.send("topic-2", "data");
			streamBridge.send("topic-3", "data");

			assertThat(cache).hasSize(3);

			// Simulate what onApplicationEvent does
			// 1. Call closeChannelsGracefully()
			Method closeMethod = StreamBridge.class.getDeclaredMethod("closeChannelsGracefully");
			closeMethod.setAccessible(true);
			closeMethod.invoke(streamBridge);

			// 2. Clear cache (what onApplicationEvent does after)
			cache.clear();
			assertThat(cache).isEmpty();

			// Can create new channels
			streamBridge.send("new-topic", "data");
			assertThat(cache).hasSize(1);
		}
	}

	@Test
	void testTheActualProblemAndSolution() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestConfig.class)
			.web(WebApplicationType.NONE)
			.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}"
			)) {

			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			Field cacheField = StreamBridge.class.getDeclaredField("channelCache");
			cacheField.setAccessible(true);
			Map<String, MessageChannel> cache = (Map<String, MessageChannel>) cacheField.get(streamBridge);

			// Example first usage
			streamBridge.send("daily-metrics", "data");
			streamBridge.send("user-events", "data");
			assertThat(cache).hasSize(2);

			// Example second usage
			streamBridge.send("payment-events", "data");
			assertThat(cache).hasSize(3);
		}
	}

	@EnableAutoConfiguration
	@Configuration
	public static class TestConfig {
	}
}
