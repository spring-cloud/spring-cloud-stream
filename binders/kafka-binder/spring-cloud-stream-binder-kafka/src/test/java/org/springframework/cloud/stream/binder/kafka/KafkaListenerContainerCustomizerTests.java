/*
 * Copyright 2024-present the original author or authors.
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

import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * @author Soby Chacko
 * @since 4.2.0
 */
@SpringBootTest(
	classes = {KafkaBinderConfiguration.class, KafkaListenerContainerCustomizerTests.TestConfig.class},
	properties = {
		"spring.cloud.function.definition=testConsumer",
		"spring.cloud.stream.bindings.testConsumer-in-0.destination=test-topic",
		"spring.cloud.stream.bindings.testConsumer-in-0.group=test-group",
		"spring.cloud.stream.kafka.bindings.testConsumer-in-0.consumer.enableDlq=true"
	}
)
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = "test-topic")
class KafkaListenerContainerCustomizerTests {

	@Autowired
	private KafkaListenerContainerCustomizer compositeCustomizer;

	@Test
	@SuppressWarnings("unchecked")
	void customizersInvoked() {
		KafkaListenerContainerCustomizer kafkaCustomizer =
			((TestConfig.CompositeCustomizer) compositeCustomizer).getKafkaCustomizer();
		ListenerContainerWithDlqAndRetryCustomizer dlqCustomizer =
			((TestConfig.CompositeCustomizer) compositeCustomizer).getDlqCustomizer();

		ArgumentCaptor<ExtendedConsumerProperties<KafkaConsumerProperties>> kafkaPropertiesCaptor = ArgumentCaptor.forClass(ExtendedConsumerProperties.class);
		ArgumentCaptor<ExtendedConsumerProperties<KafkaConsumerProperties>> dlqPropertiesCaptor = ArgumentCaptor.forClass(ExtendedConsumerProperties.class);

		verify(kafkaCustomizer, timeout(5000).times(1)).configure(
			any(AbstractMessageListenerContainer.class),
			eq("test-topic"),
			eq("test-group"),
			kafkaPropertiesCaptor.capture()
		);

		verify(dlqCustomizer, timeout(5000).times(1)).configure(
			any(AbstractMessageListenerContainer.class),
			eq("test-topic"),
			eq("test-group"),
			isNull(),
			isNull(),
			dlqPropertiesCaptor.capture()
		);

		ExtendedConsumerProperties<KafkaConsumerProperties> kafkaProperties = kafkaPropertiesCaptor.getValue();
		ExtendedConsumerProperties<KafkaConsumerProperties> dlqProperties = dlqPropertiesCaptor.getValue();

		// Assert common properties
		assertThat(kafkaProperties.getBindingName()).isEqualTo("testConsumer-in-0");
		assertThat(dlqProperties.getBindingName()).isEqualTo("testConsumer-in-0");

		// Assert Kafka-specific properties
		assertThat(kafkaProperties.getExtension())
			.satisfies(extension -> {
				assertThat(extension.isEnableDlq()).isTrue();
				assertThat(extension.isAutoRebalanceEnabled()).isTrue();
			});

		// Assert that both captured properties are the same instance
		assertThat(kafkaProperties).isSameAs(dlqProperties);
	}

	@Configuration
	@EnableAutoConfiguration
	static class TestConfig {

		@Bean
		public Consumer<String> testConsumer() {
			return message -> {
				// Do nothing, just to trigger consumer binding
			};
		}

		@Bean
		@Primary
		public KafkaListenerContainerCustomizer compositeCustomizer() {
			return new CompositeCustomizer(
				mock(KafkaListenerContainerCustomizer.class),
				mock(ListenerContainerWithDlqAndRetryCustomizer.class)
			);
		}

		static class CompositeCustomizer implements KafkaListenerContainerCustomizer {
			private final KafkaListenerContainerCustomizer kafkaCustomizer;
			private final ListenerContainerWithDlqAndRetryCustomizer dlqCustomizer;

			CompositeCustomizer(KafkaListenerContainerCustomizer kafkaCustomizer,
				ListenerContainerWithDlqAndRetryCustomizer dlqCustomizer) {
				this.kafkaCustomizer = kafkaCustomizer;
				this.dlqCustomizer = dlqCustomizer;
			}

			@Override
			public void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName,
				String group, ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
				kafkaCustomizer.configure(container, destinationName, group, extendedConsumerProperties);
				dlqCustomizer.configure(container, destinationName, group, null, null, extendedConsumerProperties);
			}

			public KafkaListenerContainerCustomizer getKafkaCustomizer() {
				return kafkaCustomizer;
			}

			public ListenerContainerWithDlqAndRetryCustomizer getDlqCustomizer() {
				return dlqCustomizer;
			}

			@Override
			public void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName, String group) {

			}
		}
	}
}
