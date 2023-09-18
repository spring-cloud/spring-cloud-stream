/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar;

import java.util.function.Function;

import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.pulsar.config.ExtendedBindingHandlerMappingsProviderConfiguration;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarExtendedBindingProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;


import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ExtendedBindingHandlerMappingsProviderConfiguration}.
 *
 * @author Soby Chacko
 */
class PulsarExtendedBindingDefaultPropertiesTests implements PulsarTestContainerSupport {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(DefaultPropertiesTestApp.class)
		.withPropertyValues(
			"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
			"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
			"--spring.cloud.stream.pulsar.binder.partitionCount: 1",
			"spring.cloud.stream.pulsar.default.consumer.schema-type: JSON",
			"spring.cloud.stream.pulsar.default.consumer.receiverQueueSize: 5000",
			"spring.cloud.stream.pulsar.default.consumer.startPaused: true",
			"spring.cloud.stream.pulsar.default.consumer.subscription.name: my-subscription",
			"spring.cloud.stream.pulsar.default.producer.schema-type: JSON",
			"spring.cloud.stream.pulsar.default.producer.blockIfQueueFull: true",
			"spring.cloud.stream.pulsar.default.producer.maxPendingMessages: 200",
			"spring.cloud.stream.pulsar.default.producer.name: my-producer");

	@Test
	void defaultsUsedWhenNoCustomBindingProperties() {
		this.contextRunner.run((context) -> {
			assertThat(context)
				.hasNotFailed()
				.hasBean("pulsar_binderProducingContext");
			ConfigurableApplicationContext pulsarBinderProducingContext =
				context.getBean("pulsar_binderProducingContext", ConfigurableApplicationContext.class);
			PulsarExtendedBindingProperties extendedBindingProperties = pulsarBinderProducingContext.getBean(PulsarExtendedBindingProperties.class);
			assertThat(extendedBindingProperties.getExtendedConsumerProperties("process-in-0"))
				.hasFieldOrPropertyWithValue("schemaType", SchemaType.JSON)
				.hasFieldOrPropertyWithValue("receiverQueueSize", 5000)
				.hasFieldOrPropertyWithValue("subscription.name", "my-subscription")
				.hasFieldOrPropertyWithValue("startPaused", true);
			assertThat(extendedBindingProperties.getExtendedProducerProperties("process-out-0"))
				.hasFieldOrPropertyWithValue("schemaType", SchemaType.JSON)
				.hasFieldOrPropertyWithValue("blockIfQueueFull", true)
				.hasFieldOrPropertyWithValue("maxPendingMessages", 200)
				.hasFieldOrPropertyWithValue("name", "my-producer");
		});
	}

	@Test
	void defaultsRespectedWhenCustomBindingProperties() {
		this.contextRunner
			.withPropertyValues(
				"spring.cloud.stream.pulsar.bindings.process-in-0.consumer.receiverQueueSize: 8000",
				"spring.cloud.stream.pulsar.bindings.process-out-0.producer.blockIfQueueFull: false",
				"spring.cloud.stream.pulsar.bindings.process-out-0.producer.maxPendingMessages: 400")
			.run((context) -> {
				assertThat(context)
					.hasNotFailed()
					.hasBean("pulsar_binderProducingContext");
				ConfigurableApplicationContext pulsarBinderProducingContext =
					context.getBean("pulsar_binderProducingContext", ConfigurableApplicationContext.class);
				PulsarExtendedBindingProperties extendedBindingProperties = pulsarBinderProducingContext.getBean(PulsarExtendedBindingProperties.class);
				assertThat(extendedBindingProperties.getExtendedConsumerProperties("process-in-0"))
					.hasFieldOrPropertyWithValue("schemaType", SchemaType.JSON)
					.hasFieldOrPropertyWithValue("receiverQueueSize", 8000)
					.hasFieldOrPropertyWithValue("subscription.name", "my-subscription")
					.hasFieldOrPropertyWithValue("startPaused", true);
				assertThat(extendedBindingProperties.getExtendedProducerProperties("process-out-0"))
					.hasFieldOrPropertyWithValue("schemaType", SchemaType.JSON)
					.hasFieldOrPropertyWithValue("blockIfQueueFull", false)
					.hasFieldOrPropertyWithValue("maxPendingMessages", 400)
					.hasFieldOrPropertyWithValue("name", "my-producer");
			});
	}


	@EnableAutoConfiguration
	static class DefaultPropertiesTestApp {

		@Bean
		public Function<String, String> process() {
			return s -> s;
		}
	}
}
