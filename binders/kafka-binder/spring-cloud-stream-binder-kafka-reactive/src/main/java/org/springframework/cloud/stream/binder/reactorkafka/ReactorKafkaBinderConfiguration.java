/*
 * Copyright 2022-2024 the original author or authors.
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

import io.micrometer.observation.ObservationRegistry;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.AdminClientConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.support.ConsumerConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.support.ProducerConfigCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Binder configuration for ReactorKafka.
 *
 * @author Gary Russell
 * @author Chris Bono
 * @author Soby Chacko
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnMissingBean(Binder.class)
@EnableConfigurationProperties({ KafkaProperties.class, KafkaExtendedBindingProperties.class })
@Import({ ReactorKafkaBinderHealthIndicatorConfiguration.class })
public class ReactorKafkaBinderConfiguration {

	/**
	 * @ConfigurationProperties is declared on the @Bean method for Spring Boot to ignore
	 * constructor binding on KafkaBinderConfigurationProperties. If constructor binding is
	 * used, it ignores all the JavaBeans style properties when generating configuration metadata.
	 *
	 * See the following issues for more details:
	 *
	 * https://github.com/spring-cloud/spring-cloud-stream/issues/2640
	 * https://github.com/spring-projects/spring-boot/issues/34031
	 *
	 * @param kafkaProperties Spring Kafka properties autoconfigured by Spring Boot
	 */
	@Bean
	@ConfigurationProperties(prefix = "spring.cloud.stream.kafka.binder")
	KafkaBinderConfigurationProperties configurationProperties(
			KafkaProperties kafkaProperties, ObjectProvider<KafkaConnectionDetails> kafkaConnectionDetails) {
		return new KafkaBinderConfigurationProperties(kafkaProperties, kafkaConnectionDetails);
	}

	@Bean
	KafkaTopicProvisioner provisioningProvider(
			KafkaBinderConfigurationProperties configurationProperties,
			ObjectProvider<AdminClientConfigCustomizer> adminClientConfigCustomizer,
			KafkaProperties kafkaProperties, ObjectProvider<KafkaConnectionDetails> kafkaConnectionDetails) {
		return new KafkaTopicProvisioner(configurationProperties, kafkaProperties,
				kafkaConnectionDetails.getIfAvailable(), adminClientConfigCustomizer.getIfUnique());
	}

	@Bean
	ReactorKafkaBinder reactorKafkaBinder(KafkaBinderConfigurationProperties configurationProperties,
										KafkaTopicProvisioner provisioningProvider,
										KafkaExtendedBindingProperties extendedBindingProperties,
										ObjectProvider<ConsumerConfigCustomizer> consumerConfigCustomizer,
										ObjectProvider<ProducerConfigCustomizer> producerConfigCustomizer,
										ObjectProvider<ReceiverOptionsCustomizer> receiverOptionsCustomizers,
										ObjectProvider<SenderOptionsCustomizer> senderOptionsptionsCustomizers,
										ObjectProvider<ObservationRegistry> observationRegistryObjectProvider) {
		ReactorKafkaBinder reactorKafkaBinder = new ReactorKafkaBinder(configurationProperties, provisioningProvider,
			observationRegistryObjectProvider.getIfUnique());
		reactorKafkaBinder.setExtendedBindingProperties(extendedBindingProperties);
		reactorKafkaBinder.setConsumerConfigCustomizer(consumerConfigCustomizer.getIfUnique());
		reactorKafkaBinder.setProducerConfigCustomizer(producerConfigCustomizer.getIfUnique());
		reactorKafkaBinder.receiverOptionsCustomizers(receiverOptionsCustomizers);
		reactorKafkaBinder.senderOptionsCustomizers(senderOptionsptionsCustomizers);
		return reactorKafkaBinder;
	}

}
