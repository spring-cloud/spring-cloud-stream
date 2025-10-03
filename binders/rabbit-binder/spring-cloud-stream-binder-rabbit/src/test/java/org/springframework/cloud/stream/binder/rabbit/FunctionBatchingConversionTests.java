/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.function.context.config.MessageConverterHelper;
import org.springframework.cloud.function.json.JacksonMapper;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.converter.MessageConversionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class FunctionBatchingConversionTests {

	@SuppressWarnings("unchecked")
	@Test
	void testBatchHeadersMatchingPayload() {
		TestChannelBinderConfiguration.applicationContextRunner(BatchFunctionConfiguration.class)
				.withPropertyValues("spring.cloud.stream.function.definition=func",
						"spring.cloud.stream.bindings.func-in-0.consumer.batch-mode=true",
						"spring.cloud.stream.rabbit.bindings.func-in-0.consumer.enable-batching=true")
				.run(context -> {
					InputDestination inputDestination = context.getBean(InputDestination.class);
					OutputDestination outputDestination = context.getBean(OutputDestination.class);

					List<byte[]> payloads = List.of("hello".getBytes(StandardCharsets.UTF_8),
							"{\"name\":\"Ricky\"}".getBytes(StandardCharsets.UTF_8),
							"{\"name\":\"Julien\"}".getBytes(StandardCharsets.UTF_8),
							"{\"name\":\"Bubbles\"}".getBytes(StandardCharsets.UTF_8),
							"hello".getBytes(StandardCharsets.UTF_8));
					List<Map<String, String>> amqpBatchHeaders = new ArrayList<>();
					for (int i = 0; i < 5; i++) {
						Map<String, String> batchHeaders = new LinkedHashMap<>();
						batchHeaders.put("amqp_receivedDeliveryMode", "PERSISTENT");
						batchHeaders.put("index", String.valueOf(i));
						amqpBatchHeaders.add(batchHeaders);
					}

					var message = MessageBuilder.withPayload(payloads)
							.setHeader("amqp_batchedHeaders", amqpBatchHeaders)
							.setHeader("deliveryAttempt", new AtomicInteger(1)).build();
					inputDestination.send(message);

					Message<byte[]> resultMessage = outputDestination.receive();
					JacksonMapper mapper = context.getBean(JacksonMapper.class);
					List<?> resultPayloads = mapper.fromJson(resultMessage.getPayload(), List.class);
					assertThat(resultPayloads).hasSize(3);

					List<Map<String, String>> amqpBatchedHeaders = (List<Map<String, String>>) resultMessage
							.getHeaders().get("amqp_batchedHeaders");
					assertThat(amqpBatchedHeaders).hasSize(resultPayloads.size());
					assertThat(amqpBatchedHeaders.get(0).get("index")).isEqualTo("1");
					assertThat(amqpBatchedHeaders.get(1).get("index")).isEqualTo("2");
					assertThat(amqpBatchedHeaders.get(2).get("index")).isEqualTo("3");

					context.stop();
				});
	}

	@Test
	void testBatchHeadersForcingFatalFailureOnConversiionException() {
		TestChannelBinderConfiguration
				.applicationContextRunner(BatchFunctionConfigurationWithAdditionalConversionHelper.class)
				.withPropertyValues("spring.cloud.stream.function.definition=func",
						"spring.cloud.stream.bindings.func-in-0.consumer.batch-mode=true",
						"spring.cloud.stream.bindings.func-in-0.consumer.max-attempts=1",
						"spring.cloud.stream.rabbit.bindings.func-in-0.consumer.enable-batching=true")
				.run(context -> {
					InputDestination inputDestination = context.getBean(InputDestination.class);

					List<byte[]> payloads = List.of("hello".getBytes(StandardCharsets.UTF_8),
							"{\"name\":\"Ricky\"}".getBytes(StandardCharsets.UTF_8),
							"{\"name\":\"Julien\"}".getBytes(StandardCharsets.UTF_8),
							"{\"name\":\"Bubbles\"}".getBytes(StandardCharsets.UTF_8),
							"hello".getBytes(StandardCharsets.UTF_8));
					List<Map<String, String>> amqpBatchHeaders = new ArrayList<>();
					for (int i = 0; i < 5; i++) {
						Map<String, String> batchHeaders = new LinkedHashMap<>();
						batchHeaders.put("amqp_receivedDeliveryMode", "PERSISTENT");
						batchHeaders.put("index", String.valueOf(i));
						amqpBatchHeaders.add(batchHeaders);
					}

					var message = MessageBuilder.withPayload(payloads)
							.setHeader("amqp_batchedHeaders", amqpBatchHeaders)
							.setHeader("deliveryAttempt", new AtomicInteger(1)).build();
					inputDestination.send(message);
					TestChannelBinder binder = context.getBean(TestChannelBinder.class);
					assertThat(binder.getLastError().getPayload()).isInstanceOf(MessageHandlingException.class);
					MessageHandlingException exception = (MessageHandlingException) binder.getLastError().getPayload();
					assertThat(exception.getCause()).isInstanceOf(MessageConversionException.class);

					context.stop();
				});
	}

	@Configuration
	@EnableAutoConfiguration
	public static class BatchFunctionConfiguration {
		@Bean
		public Function<Message<List<Person>>, Message<List<Person>>> func() {
			return x -> {
				return x;
			};
		}
	}

	@Configuration
	@EnableAutoConfiguration
	public static class BatchFunctionConfigurationWithAdditionalConversionHelper {

		@Bean
		public MessageConverterHelper helper() {
			return new MessageConverterHelper() {
				public boolean shouldFailIfCantConvert(Message<?> message) {
					return true;
				}
			};
		}

		@Bean
		public Function<Message<List<Person>>, Message<List<Person>>> func() {
			return x -> {
				return x;
			};
		}
	}

	static class Person {

		private String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String toString() {
			return "name: " + name;
		}
	}
}
