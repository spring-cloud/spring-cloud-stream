/*
 * Copyright 2017-2024 the original author or authors.
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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Integration tests to verify the bootstrap of a SpringBoot application using the Kafka binder.
 *
 * @author Marius Bogoevici
 * @author Chris Bono
 * @author Soby Chacko
 */
@EmbeddedKafka(controlledShutdown = true)
class KafkaBinderBootstrapTest {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void kafkaBinderWithStandardConfigCanStart(boolean excludeKafkaAutoConfig) {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(SimpleApplication.class)
			.web(WebApplicationType.NONE).run(
				"--spring.cloud.stream.kafka.binder.brokers=" + embeddedKafka.getBrokersAsString(),
				excludeKafkaAutoConfigParam(excludeKafkaAutoConfig))) { // @checkstyle:off
		} // @checkstyle:on

	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void kafkaBinderWithCustomConfigCanStart(boolean excludeKafkaAutoConfig) {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(SimpleApplication.class)
			.web(WebApplicationType.NONE).run(
				"--spring.cloud.stream.bindings.uppercase-in-0.destination=inputTopic",
				"--spring.cloud.stream.bindings.uppercase-in-0.group=inputGroup",
				"--spring.cloud.stream.bindings.uppercase-in-0.binder=kafka1",
				"--spring.cloud.stream.bindings.uppercase-out-0.destination=outputTopic",
				"--spring.cloud.stream.bindings.uppercase-out-0.binder=kafka2",
				"--spring.cloud.stream.binders.kafka1.type=kafka",
				"--spring.cloud.stream.binders.kafka2.type=kafka",
				"--spring.cloud.stream.binders.kafka1.environment"
					+ ".spring.cloud.stream.kafka.binder.brokers=" + embeddedKafka.getBrokersAsString(),
				"--spring.cloud.stream.binders.kafka2.environment"
					+ ".spring.cloud.stream.kafka.binder.brokers=" + embeddedKafka.getBrokersAsString(),
				excludeKafkaAutoConfigParam(excludeKafkaAutoConfig))) { // @checkstyle:off
		} // @checkstyle:on

	}

	private String excludeKafkaAutoConfigParam(boolean excludeKafkaAutoConfig) {
		return excludeKafkaAutoConfig ?
			"--spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration" : "a=a";
	}

	@SpringBootApplication
	static class SimpleApplication {

	}

}
