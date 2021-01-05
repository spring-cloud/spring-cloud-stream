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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class for Kafka-streams binder health indicator beans.
 *
 * @author Arnaud Jardiné
 */
@Configuration
@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
@ConditionalOnEnabledHealthIndicator("binders")
class KafkaStreamsBinderHealthIndicatorConfiguration {

	@Bean
	KafkaStreamsBinderHealthIndicator kafkaStreamsBinderHealthIndicator(
			ObjectProvider<KafkaStreamsRegistry> kafkaStreamsRegistry,
			@Qualifier("binderConfigurationProperties")KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties,
			KafkaProperties kafkaProperties, KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue) {
		if (kafkaStreamsRegistry.getIfUnique() != null) {
			return new KafkaStreamsBinderHealthIndicator(kafkaStreamsRegistry.getIfUnique(), kafkaStreamsBinderConfigurationProperties,
					kafkaProperties, kafkaStreamsBindingInformationCatalogue);
		}
		return null;
	}

}
