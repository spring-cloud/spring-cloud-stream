/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.reactorkafka;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.AdminClientConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Binder configuration for ReactorKafka.
 *
 * @author Gary Russell
 * @author Chris Bono
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnMissingBean(Binder.class)
@EnableConfigurationProperties({ KafkaProperties.class, KafkaExtendedBindingProperties.class })
public class ReactorKafkaBinderConfiguration {

	@Bean
	KafkaBinderConfigurationProperties configurationProperties(
			KafkaProperties kafkaProperties) {
		return new KafkaBinderConfigurationProperties(kafkaProperties);
	}

	@Bean
	KafkaTopicProvisioner provisioningProvider(
			KafkaBinderConfigurationProperties configurationProperties,
			ObjectProvider<AdminClientConfigCustomizer> adminClientConfigCustomizer, KafkaProperties kafkaProperties) {
		return new KafkaTopicProvisioner(configurationProperties,
				kafkaProperties, adminClientConfigCustomizer.getIfUnique());
	}

	@Bean
	ReactorKafkaBinder reactorKafkaBinder(KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider,
			KafkaExtendedBindingProperties extendedBindingProperties) {

		ReactorKafkaBinder reactorKafkaBinder = new ReactorKafkaBinder(configurationProperties, provisioningProvider);
		reactorKafkaBinder.setExtendedBindingProperties(extendedBindingProperties);
		return reactorKafkaBinder;
	}

}
