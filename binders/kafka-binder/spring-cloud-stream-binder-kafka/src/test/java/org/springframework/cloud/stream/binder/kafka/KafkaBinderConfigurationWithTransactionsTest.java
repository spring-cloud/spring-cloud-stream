/*
 * Copyright 2025-2025 the original author or authors.
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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.integration.IntegrationAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.kafka.config.ClientFactoryCustomizer;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@SpringBootTest(classes = { KafkaBinderConfiguration.class, IntegrationAutoConfiguration.class })
public class KafkaBinderConfigurationWithTransactionsTest {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(IntegrationAutoConfiguration.class, KafkaBinderConfiguration.class, KafkaAutoConfiguration.class)
		.withPropertyValues(
			"spring.cloud.stream.kafka.binder.transaction.transaction-id-prefix=test-tx-",
			"spring.kafka.bootstrap-servers=localhost:9092");

	@Test
	public void clientFactoryCustomizersAppliedToTransactionManager() {
		contextRunner.withUserConfiguration(TransactionClientFactoryCustomizerConfig.class)
			.run(context -> {
				assertThat(context).hasSingleBean(KafkaMessageChannelBinder.class);
				KafkaMessageChannelBinder kafkaMessageChannelBinder =
					context.getBean(KafkaMessageChannelBinder.class);

				Map<String, ClientFactoryCustomizer> customizers =
					context.getBeansOfType(ClientFactoryCustomizer.class);
				assertThat(customizers).hasSize(1);

				Field transactionManagerField = ReflectionUtils.findField(
					KafkaMessageChannelBinder.class, "transactionManager",
					KafkaTransactionManager.class);
				assertThat(transactionManagerField).isNotNull();
				ReflectionUtils.makeAccessible(transactionManagerField);
				KafkaTransactionManager<?, ?> transactionManager =
					(KafkaTransactionManager<?, ?>) ReflectionUtils.getField(
						transactionManagerField, kafkaMessageChannelBinder);

				assertThat(transactionManager).isNotNull();

				ProducerFactory<?, ?> producerFactory = transactionManager.getProducerFactory();
				assertThat(producerFactory).isNotNull();

				// Verify customizer was applied - check if our flag was set
				TransactionClientFactoryCustomizerConfig config =
					context.getBean(TransactionClientFactoryCustomizerConfig.class);
				assertThat(config.wasCustomizerApplied()).isTrue();
				assertThat(config.getCustomizedFactories()).contains(producerFactory);
			});
	}

	@Configuration
	static class TransactionClientFactoryCustomizerConfig {
		private final List<ProducerFactory<?, ?>> customizedFactories = new ArrayList<>();
		private boolean customizerApplied = false;

		@Bean
		ClientFactoryCustomizer testClientFactoryCustomizer() {
			return new ClientFactoryCustomizer() {
				@Override
				public void configure(ProducerFactory<?, ?> pf) {
					customizerApplied = true;
					customizedFactories.add(pf);
				}
			};
		}

		public boolean wasCustomizerApplied() {
			return customizerApplied;
		}

		public List<ProducerFactory<?, ?>> getCustomizedFactories() {
			return customizedFactories;
		}
	}
}
