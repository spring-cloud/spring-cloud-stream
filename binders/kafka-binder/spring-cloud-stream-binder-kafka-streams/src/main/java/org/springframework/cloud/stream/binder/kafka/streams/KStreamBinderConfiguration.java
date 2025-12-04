/*
 * Copyright 2017-present the original author or authors.
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
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.AdminClientConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Configuration for KStream binder.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Soby Chacko
 * @author Chris Bono
 */
@Configuration
@Import({ MultiBinderPropertiesConfiguration.class,
		KafkaStreamsBinderHealthIndicatorConfiguration.class,
		KafkaStreamsJaasConfiguration.class})
@EnableConfigurationProperties(KafkaProperties.class)
public class KStreamBinderConfiguration {

	@Bean
	public KafkaTopicProvisioner provisioningProvider(
			KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties,
			KafkaProperties kafkaProperties, ObjectProvider<KafkaConnectionDetails> kafkaConnectionDetails,
			ObjectProvider<AdminClientConfigCustomizer> adminClientConfigCustomizer) {
		return new KafkaTopicProvisioner(kafkaStreamsBinderConfigurationProperties,
				kafkaProperties, kafkaConnectionDetails.getIfAvailable(), adminClientConfigCustomizer.getIfUnique());
	}

	@Bean
	public KStreamBinder kStreamBinder(
			KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
			KafkaTopicProvisioner kafkaTopicProvisioner,
			KafkaStreamsMessageConversionDelegate KafkaStreamsMessageConversionDelegate,
			KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue,
			KeyValueSerdeResolver keyValueSerdeResolver,
			KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties, KafkaStreamsRegistry kafkaStreamsRegistry) {
		KStreamBinder kStreamBinder = new KStreamBinder(binderConfigurationProperties,
				kafkaTopicProvisioner, KafkaStreamsMessageConversionDelegate,
				KafkaStreamsBindingInformationCatalogue, keyValueSerdeResolver, kafkaStreamsRegistry);
		kStreamBinder.setKafkaStreamsExtendedBindingProperties(
				kafkaStreamsExtendedBindingProperties);
		return kStreamBinder;
	}

	@Bean
	@ConditionalOnBean(name = "outerContext")
	public static BeanFactoryPostProcessor outerContextBeanFactoryPostProcessor() {
		return beanFactory -> {

			// It is safe to call getBean("outerContext") here, because this bean is
			// registered as first
			// and as independent from the parent context.
			ApplicationContext outerContext = (ApplicationContext) beanFactory
					.getBean("outerContext");
			beanFactory.registerSingleton(
					KafkaStreamsMessageConversionDelegate.class.getSimpleName(),
					outerContext.getBean(KafkaStreamsMessageConversionDelegate.class));
			beanFactory.registerSingleton(
					KafkaStreamsBindingInformationCatalogue.class.getSimpleName(),
					outerContext.getBean(KafkaStreamsBindingInformationCatalogue.class));
			beanFactory.registerSingleton(KeyValueSerdeResolver.class.getSimpleName(),
					outerContext.getBean(KeyValueSerdeResolver.class));
			beanFactory.registerSingleton(
					KafkaStreamsExtendedBindingProperties.class.getSimpleName(),
					outerContext.getBean(KafkaStreamsExtendedBindingProperties.class));
			beanFactory.registerSingleton(
					KafkaStreamsRegistry.class.getSimpleName(),
					outerContext.getBean(KafkaStreamsRegistry.class));
		};
	}

}
