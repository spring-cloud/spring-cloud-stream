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

package org.springframework.cloud.stream.binder.rabbit;

import java.lang.reflect.Method;

import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * GH-2902: the observation can be opted out of, via the {@code enableObservation} binder
 * property.
 *
 * @since 5.0.6
 */
class RabbitObservationEnablementTests {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(PropertiesConfiguration.class);

	@Test
	void observationIsEnabledByDefault() {
		this.contextRunner.run((context) -> assertThat(
				context.getBean(RabbitBinderConfigurationProperties.class).isEnableObservation()).isTrue());
	}

	@Test
	void observationCanBeDisabled() {
		this.contextRunner.withPropertyValues("spring.cloud.stream.rabbit.binder.enable-observation=false")
			.run((context) -> assertThat(
					context.getBean(RabbitBinderConfigurationProperties.class).isEnableObservation()).isFalse());
	}

	@Test
	void templateObservationIsEnabledByDefaultWithObservationRegistry() throws Exception {
		assertThat(observationEnabled(new RabbitMessageChannelBinder(mock(ConnectionFactory.class),
				new RabbitProperties(), null), true)).isTrue();
	}

	@Test
	void templateObservationIsDisabledWhenOptedOut() throws Exception {
		RabbitMessageChannelBinder binder = new RabbitMessageChannelBinder(mock(ConnectionFactory.class),
				new RabbitProperties(), null);
		binder.setEnableObservation(false);
		assertThat(observationEnabled(binder, true)).isFalse();
	}

	@Test
	void templateObservationIsNotEnabledWithoutObservationRegistry() throws Exception {
		assertThat(observationEnabled(new RabbitMessageChannelBinder(mock(ConnectionFactory.class),
				new RabbitProperties(), null), false)).isFalse();
	}

	private boolean observationEnabled(RabbitMessageChannelBinder binder, boolean withObservationRegistry)
			throws Exception {
		try (GenericApplicationContext context = new GenericApplicationContext()) {
			if (withObservationRegistry) {
				context.registerBean(ObservationRegistry.class, ObservationRegistry::create);
			}
			context.refresh();
			binder.setApplicationContext(context);
			Method buildRabbitTemplate = ReflectionUtils.findMethod(RabbitMessageChannelBinder.class,
					"buildRabbitTemplate", RabbitProducerProperties.class, boolean.class);
			ReflectionUtils.makeAccessible(buildRabbitTemplate);
			RabbitTemplate template = (RabbitTemplate) ReflectionUtils.invokeMethod(buildRabbitTemplate, binder,
					new RabbitProducerProperties(), false);
			return (boolean) ReflectionTestUtils.getField(template, "observationEnabled");
		}
	}

	@Configuration(proxyBeanMethods = false)
	@EnableConfigurationProperties(RabbitBinderConfigurationProperties.class)
	static class PropertiesConfiguration {

		@Bean
		ObservationRegistry observationRegistry() {
			return ObservationRegistry.create();
		}

	}

}
