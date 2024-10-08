/*
 * Copyright 2024-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.dlq;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 *
 * @since 4.2
 */
@SpringBootTest(properties = { "spring.cloud.stream.function.bindings.listener-in-0=input",
		"spring.cloud.stream.bindings.input.destination=myDestination",
		"spring.cloud.stream.bindings.input.group=consumerGroup",
		"spring.cloud.stream.bindings.input.consumer.max-attempts=1",
		"spring.cloud.stream.rabbit.bindings.input.consumer.auto-bind-dlq=true",
		"spring.cloud.stream.rabbit.bindings.input.consumer.dlq-ttl=1000",
		"spring.cloud.stream.rabbit.bindings.input.consumer.dlq-dead-letter-exchange=" })
@DirtiesContext
public class RabbitDlqTests {

	@Test
	void verifyServerSideRetry(@Autowired RabbitTemplate rabbitTemplate, @Autowired CountDownLatch dlqRetryExhausted)
			throws InterruptedException {

		rabbitTemplate.convertAndSend("myDestination.consumerGroup", "test data");

		assertThat(dlqRetryExhausted.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	public static class RabbitDlqConfiguration {

		@Bean
		@ServiceConnection
		RabbitMQContainer rabbitContainer() {
			return new RabbitMQContainer(DockerImageName.parse("rabbitmq:4"));
		}

		@Bean
		CountDownLatch dlqRetryExhausted() {
			return new CountDownLatch(1);
		}

		@Bean
		public Consumer<Message<String>> listener(CountDownLatch dlqRetryExhausted) {
			return message -> {
				Long retryCount = message.getHeaders().get(AmqpHeaders.RETRY_COUNT, Long.class);
				if (retryCount != null && retryCount.equals(3L)) {
					dlqRetryExhausted.countDown();
					// giving up - don't send to DLX
					throw new ImmediateAcknowledgeAmqpException("Failed after 4 attempts");
				}
				throw new AmqpRejectAndDontRequeueException("failed");
			};
		}

	}

	@Nested
	@TestPropertySource(properties = "spring.cloud.stream.rabbit.bindings.input.consumer.republish-to-dlq=false")
	class NoRepublishToDlx {

		@Test
		void verifyServerSideRetry(@Autowired RabbitTemplate rabbitTemplate,
				@Autowired CountDownLatch dlqRetryExhausted) throws InterruptedException {

			rabbitTemplate.convertAndSend("myDestination.consumerGroup", "test data");

			assertThat(dlqRetryExhausted.await(10, TimeUnit.SECONDS)).isTrue();
		}

	}

}
