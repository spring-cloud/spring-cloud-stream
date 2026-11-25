/*
 * Copyright 2026-present the original author or authors.
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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealth;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealthIndicator;
import org.springframework.cloud.stream.config.BindersHealthIndicatorAutoConfiguration.BindersHealthContributor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for custom {@link KafkaBinderHealth} beans when multiple binders are used.
 *
 * @author Oleg Zhurakousky
 */
@EmbeddedKafka(count = 1, controlledShutdown = true)
class KafkaBinderMultiBinderCustomHealthTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	private ConfigurableApplicationContext run(Class<?> app, String... extra) {
		String[] common = {
				"--spring.cloud.stream.bindings.uppercase-in-0.destination=inputTopic",
				"--spring.cloud.stream.bindings.uppercase-in-0.group=inputGroup",
				"--spring.cloud.stream.bindings.uppercase-in-0.binder=kafka1",
				"--spring.cloud.stream.bindings.uppercase-out-0.destination=outputTopic",
				"--spring.cloud.stream.bindings.uppercase-out-0.binder=kafka2",
				"--spring.cloud.stream.binders.kafka1.type=kafka",
				"--spring.cloud.stream.binders.kafka2.type=kafka",
				"--spring.cloud.stream.binders.kafka1.environment.spring.cloud.stream.kafka.binder.brokers="
						+ embeddedKafka.getBrokersAsString(),
				"--spring.cloud.stream.binders.kafka2.environment.spring.cloud.stream.kafka.binder.brokers="
						+ embeddedKafka.getBrokersAsString()};
		String[] all = new String[common.length + extra.length];
		System.arraycopy(common, 0, all, 0, common.length);
		System.arraycopy(extra, 0, all, common.length, extra.length);
		return new SpringApplicationBuilder(app).web(WebApplicationType.NONE).run(all);
	}

	@Test
	void defaultIndicatorIsCreatedForEachBinder() {
		try (ConfigurableApplicationContext context = run(DefaultApp.class)) {
			BindersHealthContributor contributor = context.getBean(BindersHealthContributor.class);
			assertThat(contributor.getContributor("kafka1")).isInstanceOf(KafkaBinderHealthIndicator.class);
			assertThat(contributor.getContributor("kafka2")).isInstanceOf(KafkaBinderHealthIndicator.class);
		}
	}

	@Test
	void customIndicatorInApplicationContextIsNotUsedByBinders() {
		try (ConfigurableApplicationContext context = run(CustomApp.class)) {
			BindersHealthContributor contributor = context.getBean(BindersHealthContributor.class);
			// binders have their own context, so the default indicators are still created
			assertThat(contributor.getContributor("kafka1")).isInstanceOf(KafkaBinderHealthIndicator.class);
			assertThat(contributor.getContributor("kafka2")).isInstanceOf(KafkaBinderHealthIndicator.class);
			assertThat(context.getBean(KafkaBinderHealth.class)).isNotInstanceOf(KafkaBinderHealthIndicator.class);
		}
	}

	@Test
	void customIndicatorInBinderContextReplacesOnlyThatBindersIndicator() {
		try (ConfigurableApplicationContext context = run(DefaultApp.class,
				"--spring.cloud.stream.binders.kafka1.environment.spring.main.sources="
						+ BinderCustomHealthConfig.class.getName())) {
			BindersHealthContributor contributor = context.getBean(BindersHealthContributor.class);
			HealthContributor kafka1 = contributor.getContributor("kafka1");
			assertThat(kafka1).isInstanceOf(KafkaBinderHealth.class).isNotInstanceOf(KafkaBinderHealthIndicator.class);
			assertThat(((HealthIndicator) kafka1).health().getDetails()).containsEntry("custom", true);
			assertThat(contributor.getContributor("kafka2")).isInstanceOf(KafkaBinderHealthIndicator.class);
		}
	}

	@Configuration
	static class BinderCustomHealthConfig {

		@Bean
		public KafkaBinderHealth kafkaBinderHealthIndicator() {
			return () -> Health.up().withDetail("custom", true).build();
		}

	}

	@EnableAutoConfiguration
	@Configuration
	static class DefaultApp {

		@Bean
		public Function<String, String> uppercase() {
			return v -> v;
		}

	}

	@EnableAutoConfiguration
	@Configuration
	static class CustomApp {

		@Bean
		public Function<String, String> uppercase() {
			return v -> v;
		}

		@Bean
		public KafkaBinderHealth kafkaBinderHealthIndicator() {
			return () -> Health.up().withDetail("custom", true).build();
		}

	}

}
