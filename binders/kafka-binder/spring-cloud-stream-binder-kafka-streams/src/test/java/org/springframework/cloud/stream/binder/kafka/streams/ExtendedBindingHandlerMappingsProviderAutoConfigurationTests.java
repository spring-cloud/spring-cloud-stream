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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ExtendedBindingHandlerMappingsProviderAutoConfiguration}.
 */
class ExtendedBindingHandlerMappingsProviderAutoConfigurationTests {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(KafkaStreamsTestApp.class)
			.withPropertyValues(
					"spring.cloud.function.ineligible-definitions: sendToDlqAndContinue",
					"spring.cloud.stream.kafka.streams.default.consumer.application-id: testApp123",
					"spring.cloud.stream.kafka.streams.default.consumer.consumed-as: default-consumer",
					"spring.cloud.stream.kafka.streams.default.consumer.materialized-as: default-materializer",
					"spring.cloud.stream.kafka.streams.default.producer.produced-as: default-producer",
					"spring.cloud.stream.kafka.streams.default.producer.key-serde: default-foo");

	@Test
	void defaultsUsedWhenNoCustomBindingProperties() {
		this.contextRunner.run((context) -> {
			assertThat(context)
					.hasNotFailed()
					.hasSingleBean(KafkaStreamsExtendedBindingProperties.class);
			KafkaStreamsExtendedBindingProperties extendedBindingProperties = context.getBean(KafkaStreamsExtendedBindingProperties.class);
			assertThat(extendedBindingProperties.getExtendedConsumerProperties("process-in-0"))
					.hasFieldOrPropertyWithValue("applicationId", "testApp123")
					.hasFieldOrPropertyWithValue("consumedAs", "default-consumer")
					.hasFieldOrPropertyWithValue("materializedAs", "default-materializer");
			assertThat(extendedBindingProperties.getExtendedProducerProperties("process-out-0"))
					.hasFieldOrPropertyWithValue("producedAs", "default-producer")
					.hasFieldOrPropertyWithValue("keySerde", "default-foo");
		});
	}

	@Test
	void defaultsRespectedWhenCustomBindingProperties() {
		this.contextRunner
				.withPropertyValues(
					"spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.consumed-as: custom-consumer",
					"spring.cloud.stream.kafka.streams.bindings.process-out-0.producer.produced-as: custom-producer")
				.run((context) -> {
					assertThat(context)
							.hasNotFailed()
							.hasSingleBean(KafkaStreamsExtendedBindingProperties.class);
					KafkaStreamsExtendedBindingProperties extendedBindingProperties = context.getBean(KafkaStreamsExtendedBindingProperties.class);
					assertThat(extendedBindingProperties.getExtendedConsumerProperties("process-in-0"))
							.hasFieldOrPropertyWithValue("applicationId", "testApp123")
							.hasFieldOrPropertyWithValue("consumedAs", "custom-consumer")
							.hasFieldOrPropertyWithValue("materializedAs", "default-materializer");
					assertThat(extendedBindingProperties.getExtendedProducerProperties("process-out-0"))
							.hasFieldOrPropertyWithValue("producedAs", "custom-producer")
							.hasFieldOrPropertyWithValue("keySerde", "default-foo");
				});
	}

	@EnableAutoConfiguration
	static class KafkaStreamsTestApp {
	}

}
