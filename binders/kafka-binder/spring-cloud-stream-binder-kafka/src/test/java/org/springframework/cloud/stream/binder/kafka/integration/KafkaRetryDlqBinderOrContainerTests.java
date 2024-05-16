/*
 * Copyright 2021-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.kafka.ListenerContainerWithDlqAndRetryCustomizer;
import org.springframework.cloud.stream.binder.kafka.support.ProducerConfigCustomizer;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.backoff.BackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Soby Chacko
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.cloud.function.definition=retryInBinder;retryInContainer",
		"spring.cloud.stream.bindings.retryInBinder-in-0.group=foo",
		"spring.cloud.stream.bindings.retryInContainer-in-0.group=bar",
		"spring.cloud.stream.kafka.bindings.retryInBinder-in-0.consumer.enable-dlq=true",
		"spring.cloud.stream.kafka.bindings.retryInContainer-in-0.consumer.enable-dlq=true"})
@EmbeddedKafka
@DirtiesContext
class KafkaRetryDlqBinderOrContainerTests {

	private static final CountDownLatch latch = new CountDownLatch(2);

	@Test
	void retryAndDlqInRightPlace(@Autowired BindingsLifecycleController controller) throws Exception {
		Binding<?> retryInBinder = controller.queryState("retryInBinder-in-0").get(0);
		assertThat(KafkaTestUtils.getPropertyValue(retryInBinder, "lifecycle.retryTemplate")).isNotNull();
		assertThat(KafkaTestUtils.getPropertyValue(retryInBinder,
				"lifecycle.messageListenerContainer.commonErrorHandler")).isNull();
		Binding<?> retryInContainer = controller.queryState("retryInContainer-in-0").get(0);
		assertThat(KafkaTestUtils.getPropertyValue(retryInContainer, "lifecycle.retryTemplate")).isNull();
		assertThat(KafkaTestUtils.getPropertyValue(retryInContainer,
				"lifecycle.messageListenerContainer.commonErrorHandler")).isInstanceOf(CommonErrorHandler.class);
		assertThat(KafkaTestUtils.getPropertyValue(retryInContainer,
					"lifecycle.messageListenerContainer.commonErrorHandler.failureTracker.backOff"))
				.isInstanceOf(ExponentialBackOffWithMaxRetries.class);
		boolean await = latch.await(5, TimeUnit.SECONDS);
		assertThat(await).isTrue();

	}

	@EnableAutoConfiguration
	@Configuration
	public static class ConfigCustomizerTestConfig {

		@Bean
		public Consumer<String> retryInBinder() {
			return str -> { };
		}

		@Bean
		public Consumer<String> retryInContainer() {
			return str -> { };
		}

		@Bean
		ListenerContainerWithDlqAndRetryCustomizer cust() {
			return new ListenerContainerWithDlqAndRetryCustomizer() {

				@Override
				public void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName,
						String group,
						BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> dlqDestinationResolver,
						@Nullable BackOff backOff) {

					if (destinationName.contains("Container")) {
						ConsumerRecordRecoverer dlpr = new DeadLetterPublishingRecoverer(mock(KafkaOperations.class),
								dlqDestinationResolver);
						container.setCommonErrorHandler(new DefaultErrorHandler(dlpr, backOff));
					}
				}

				@Override
				public boolean retryAndDlqInBinding(String destinationName, String group) {
					return !destinationName.contains("Container");
				}

			};
		}

		// Because we have DLQ enabled on the consumer binding,
		// this ProducerConfigCustomizer is used by the producer on DLQ.
		@Bean
		public ProducerConfigCustomizer producerConfigCustomizer() {
			return (producerProperties, binding, destination) -> {
				if (binding.equals("retryInBinder-in-0")) {
					latch.countDown();
				}
				else if (binding.equals("retryInContainer-in-0")) {
					latch.countDown();
				}
			};
		}
	}

}
