/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderMetrics;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@ConditionalOnBean(name = "outerContext")
@ConditionalOnMissingBean(KafkaBinderMetrics.class)
@ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
public class KafkaBinderMetricsConfigurationWithMultiBinder {

	@Bean
	public MeterBinder kafkaBinderMetrics(
			KafkaBinderConfigurationProperties configurationProperties,
			ConfigurableApplicationContext context) {

		MeterRegistry meterRegistry = context.getBean("outerContext", ApplicationContext.class)
				.getBean(MeterRegistry.class);
		return new KafkaBinderMetrics(configurationProperties, null, meterRegistry);
	}

	@ConditionalOnClass(name = "org.springframework.kafka.core.MicrometerConsumerListener")
	protected class KafkaMicrometer {

		@Bean
		@ConditionalOnMissingBean(name = "binderClientFactoryCustomizer")
		public ClientFactoryCustomizer binderClientFactoryCustomizer(ConfigurableApplicationContext context) {


			return new ClientFactoryCustomizer() {

				MeterRegistry meterRegistry = context.getBean("outerContext", ApplicationContext.class)
						.getBean(MeterRegistry.class);

				@Override
				public void configure(ProducerFactory<?, ?> pf) {
					if (pf instanceof DefaultKafkaProducerFactory) {
						((DefaultKafkaProducerFactory<?, ?>) pf)
								.addListener(new MicrometerProducerListener<>(this.meterRegistry));
					}
				}

				@Override
				public void configure(ConsumerFactory<?, ?> cf) {
					if (cf instanceof DefaultKafkaConsumerFactory) {
						((DefaultKafkaConsumerFactory<?, ?>) cf)
								.addListener(new MicrometerConsumerListener<>(this.meterRegistry));
					}
				}

			};

		}

	}

}
