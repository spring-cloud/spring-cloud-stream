/*
 * Copyright 2022-2024 the original author or authors.
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

import java.util.function.Consumer;

import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests for {@link KafkaStreamsBinderEnvironmentPostProcessor}.
 *
 * @author Chris Bono
 */
@EmbeddedKafka
class KafkaStreamsBinderEnvironmentPostProcessorTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	void defaultIneligibleFunctionIsSet() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(KafkaStreamsBinderEnvironmentPostProcessorTests.SimpleKafkaStreamsApplication.class)
				.web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.kafka.streams.binder.brokers="
								+ embeddedKafka.getBrokersAsString())) {
			assertThat(context.getEnvironment().getProperty("spring.cloud.function.ineligible-definitions"))
					.isEqualTo("sendToDlqAndContinue");
		}
	}

	@Test
	void userSpecifiedIneligibleFunctionIsAppendedToDefaultList() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(KafkaStreamsBinderEnvironmentPostProcessorTests.SimpleKafkaStreamsApplication.class)
				.web(WebApplicationType.NONE).run(
						"--spring.cloud.function.ineligible-definitions=foo",
						"--spring.cloud.stream.kafka.streams.binder.brokers="
								+ embeddedKafka.getBrokersAsString())) {
			assertThat(context.getEnvironment().getProperty("spring.cloud.function.ineligible-definitions"))
					.isEqualTo("sendToDlqAndContinue,foo");
		}
	}

	@EnableAutoConfiguration
	static class SimpleKafkaStreamsApplication {
		@Bean
		public Consumer<KStream<Object, String>> logInput() {
			return s -> {
				// No-op consumer
			};
		}
	}
}
