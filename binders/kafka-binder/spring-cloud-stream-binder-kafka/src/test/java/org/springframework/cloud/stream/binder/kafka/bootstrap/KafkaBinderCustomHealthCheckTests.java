/*
 * Copyright 2022-2025 the original author or authors.
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
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.health.contributor.Health;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealth;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealthIndicator;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * @author Soby Chacko
 */
@EmbeddedKafka(count = 1, controlledShutdown = true)
class KafkaBinderCustomHealthCheckTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	void customHealthIndicatorIsActivated() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				CustomHealthCheckApplication.class).web(WebApplicationType.NONE).run(
				"--spring.cloud.stream.kafka.binder.brokers="
						+ embeddedKafka.getBrokersAsString());
		final KafkaBinderHealth kafkaBinderHealth = applicationContext.getBean(KafkaBinderHealth.class);
		assertThat(kafkaBinderHealth).isInstanceOf(CustomHealthIndicator.class);
		assertThatThrownBy(() -> applicationContext.getBean(KafkaBinderHealthIndicator.class)).isInstanceOf(NoSuchBeanDefinitionException.class);
		applicationContext.close();
	}

	@EnableAutoConfiguration
	@Configuration
	static class CustomHealthCheckApplication {

		@Bean
		public CustomHealthIndicator kafkaBinderHealthIndicator() {
			return new CustomHealthIndicator();
		}
	}

	static class CustomHealthIndicator implements KafkaBinderHealth {

		@Override
		public Health health() {
			return null;
		}
	}
}
