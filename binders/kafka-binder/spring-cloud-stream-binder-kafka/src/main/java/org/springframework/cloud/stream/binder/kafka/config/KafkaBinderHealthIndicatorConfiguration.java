/*
 * Copyright 2018-2022 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealth;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Configuration class for Kafka binder health indicator beans.
 *
 * @author Oleg Zhurakousky
 * @author Chukwubuikem Ume-Ugwa
 * @author Soby Chacko
 */

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
@ConditionalOnEnabledHealthIndicator("binders")
@ConditionalOnMissingBean(KafkaBinderHealth.class)
public class KafkaBinderHealthIndicatorConfiguration {

	@Bean
	public KafkaBinderHealthIndicator kafkaBinderHealthIndicator(
			KafkaMessageChannelBinder kafkaMessageChannelBinder,
			KafkaBinderConfigurationProperties configurationProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class);
		Map<String, Object> mergedConfig = configurationProperties
				.mergedConsumerConfiguration();
		if (!ObjectUtils.isEmpty(mergedConfig)) {
			props.putAll(mergedConfig);
		}
		if (!props.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					configurationProperties.getKafkaConnectionString());
		}
		String healthIndicatorConsumerGroup = configurationProperties.getHealthIndicatorConsumerGroup();
		if (StringUtils.hasText(healthIndicatorConsumerGroup)) {
			props.put(ConsumerConfig.GROUP_ID_CONFIG, healthIndicatorConsumerGroup);
		}
		ConsumerFactory<?, ?> consumerFactory = new DefaultKafkaConsumerFactory<>(props);
		KafkaBinderHealthIndicator indicator = new KafkaBinderHealthIndicator(
				kafkaMessageChannelBinder, consumerFactory);
		indicator.setTimeout(configurationProperties.getHealthTimeout());
		indicator.setConsiderDownWhenAnyPartitionHasNoLeader(configurationProperties.isConsiderDownWhenAnyPartitionHasNoLeader());
		return indicator;
	}

}
