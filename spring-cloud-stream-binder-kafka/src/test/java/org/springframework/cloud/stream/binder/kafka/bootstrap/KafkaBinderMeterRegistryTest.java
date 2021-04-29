/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.bootstrap;

import java.util.function.Function;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Soby Chacko
 */
public class KafkaBinderMeterRegistryTest {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 10);

	@Test
	public void testMetricsWithSingleBinder() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(SimpleApplication.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.uppercase-in-0.destination=inputTopic",
						"--spring.cloud.stream.bindings.uppercase-in-0.group=inputGroup",
						"--spring.cloud.stream.bindings.uppercase-out-0.destination=outputTopic",
						"--spring.cloud.stream.kafka.binder.brokers" + "="
								+ embeddedKafka.getEmbeddedKafka().getBrokersAsString());

		final MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry.class);
		assertMeterRegistry(meterRegistry);
		applicationContext.close();
	}

	@Test
	public void testMetricsWithMultiBinders() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(SimpleApplication.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.uppercase-in-0.destination=inputTopic",
						"--spring.cloud.stream.bindings.uppercase-in-0.group=inputGroup",
						"--spring.cloud.stream.bindings.uppercase-in-0.binder=kafka1",
						"--spring.cloud.stream.bindings.uppercase-out-0.destination=outputTopic",
						"--spring.cloud.stream.bindings.uppercase-out-0.binder=kafka2",
						"--spring.cloud.stream.binders.kafka1.type=kafka",
						"--spring.cloud.stream.binders.kafka2.type=kafka",
						"--spring.cloud.stream.binders.kafka1.environment"
								+ ".spring.cloud.stream.kafka.binder.brokers" + "="
								+ embeddedKafka.getEmbeddedKafka().getBrokersAsString(),
						"--spring.cloud.stream.binders.kafka2.environment"
								+ ".spring.cloud.stream.kafka.binder.brokers" + "="
								+ embeddedKafka.getEmbeddedKafka().getBrokersAsString());

		final MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry.class);
		assertMeterRegistry(meterRegistry);
		applicationContext.close();
	}

	private void assertMeterRegistry(MeterRegistry meterRegistry) {
		assertThat(meterRegistry).isNotNull();

		// assert kafka binder metrics
		assertThat(meterRegistry.get("spring.cloud.stream.binder.kafka.offset")
				.tag("group", "inputGroup")
				.tag("topic", "inputTopic").gauge().value()).isNotNull();

		// assert consumer metrics
		assertThatCode(() -> meterRegistry.get("kafka.consumer.connection.count").meter()).doesNotThrowAnyException();

		// assert producer metrics
		assertThatCode(() -> meterRegistry.get("kafka.producer.connection.count").meter()).doesNotThrowAnyException();
	}

	@SpringBootApplication
	static class SimpleApplication {

		@Bean
		public Function<String, String> uppercase() {
			return String::toUpperCase;
		}
	}
}
