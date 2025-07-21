/*
 * Copyright 2023-2025 the original author or authors.
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

import java.lang.reflect.Field;
import java.util.Map;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderMetrics;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests to verify that custom configurations defined in spring.main.sources
 * are properly loaded before the default binder configuration in multi-binder scenarios.
 *
 * @author Fernando Blanch
 * @since 4.1.0
 */
@SpringBootTest(classes = KafkaMultiBinderCustomConfigurationTests.class,
		webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.cloud.stream.defaultBinder=kafka1",
		"spring.cloud.stream.binders.kafka1.type=kafka",
		"spring.cloud.stream.binders.kafka1.environment.spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}",
		"spring.cloud.stream.binders.kafka2.type=kafka",
		"spring.cloud.stream.binders.kafka2.environment.spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}",
		"spring.cloud.stream.binders.kafka2.environment.spring.main.sources=" +
				"org.springframework.cloud.stream.binder.kafka.integration.KafkaMultiBinderCustomConfigurationTests$CustomConfiguration"
})
@DirtiesContext
@EnableAutoConfiguration
@EmbeddedKafka(controlledShutdown = true)
class KafkaMultiBinderCustomConfigurationTests {

	@Autowired
	private DefaultBinderFactory binderFactory;

	/**
	 * Verifies that the custom user configuration is loaded from spring.main.sources.
	 */
	@Test
	void binderKafka2UsesCustomConfigurationIsLoadedFromSpringMainSources() throws IllegalAccessException {
		// Force initialization of binders
		Binder<?, ?, ?> kafka2Binder = binderFactory.getBinder("kafka2", Object.class);
		assertThat(kafka2Binder).isNotNull();

		// Get the kafka2 binder context
		ConfigurableApplicationContext kafka2Context = getBinderContext("kafka2");
		assertThat(kafka2Context).isNotNull();

		// Verify that our custom bean is used instead of the default one
		KafkaBinderMetrics kafkaBinderMetrics = kafka2Context.getBean(KafkaBinderMetrics.class);
		assertThat(kafkaBinderMetrics).isInstanceOf(CustomKafkaBinderMetrics.class);
	}

	/**
	 * Verifies that the default configuration is used when no custom user configuration is provided.
	 */
	@Test
	void binderKafka1UsesDefaultBeanFromKafkaBinderMetricsConfigurationWithMultiBinder() throws IllegalAccessException {
		// Force initialization of binders
		Binder<?, ?, ?> kafka1Binder = binderFactory.getBinder("kafka1", Object.class);
		assertThat(kafka1Binder).isNotNull();

		ConfigurableApplicationContext kafka1Context = getBinderContext("kafka1");
		assertThat(kafka1Context).isNotNull();

		// Verify that the metrics bean is from KafkaBinderMetricsConfigurationWithMultiBinder configuration
		// (not a CustomKafkaBinderMetrics instance)
		KafkaBinderMetrics kafka1BinderMetrics = kafka1Context.getBean(KafkaBinderMetrics.class);
		assertThat(kafka1BinderMetrics).isNotInstanceOf(CustomKafkaBinderMetrics.class);
	}

	/**
	 * Helper method to get the binder context from the binderInstanceCache field in DefaultBinderFactory.
	 */
	private ConfigurableApplicationContext getBinderContext(String binderName) throws IllegalAccessException {
		Field binderInstanceCacheField = ReflectionUtils.findField(DefaultBinderFactory.class, "binderInstanceCache");
		assertThat(binderInstanceCacheField).isNotNull();
		ReflectionUtils.makeAccessible(binderInstanceCacheField);
		@SuppressWarnings("unchecked")
		Map<String, Map.Entry<Binder<?, ?, ?>, ConfigurableApplicationContext>> binderInstanceCache =
				(Map<String, Map.Entry<Binder<?, ?, ?>, ConfigurableApplicationContext>>) binderInstanceCacheField.get(this.binderFactory);
		return binderInstanceCache.get(binderName).getValue();
	}

	/**
	 * Custom configuration that provides a custom KafkaBinderMetrics.
	 */
	static class CustomConfiguration {

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		KafkaBinderMetrics kafkaBinderMetrics(KafkaMessageChannelBinder kafkaMessageChannelBinder,
				KafkaBinderConfigurationProperties configurationProperties,
				MeterRegistry meterRegistry) {
			return new CustomKafkaBinderMetrics(kafkaMessageChannelBinder, configurationProperties, meterRegistry);
		}

	}

	static class CustomKafkaBinderMetrics extends KafkaBinderMetrics {

		CustomKafkaBinderMetrics(KafkaMessageChannelBinder binder,
				KafkaBinderConfigurationProperties binderConfigurationProperties,
				MeterRegistry meterRegistry) {
			super(binder, binderConfigurationProperties, null, meterRegistry);
		}

	}

}
