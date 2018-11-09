/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Configuration for KStream binder.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Soby Chacko
 */
@Configuration
@Import({KafkaAutoConfiguration.class, KStreamBinderConfiguration.KStreamMissingBeansRegistrar.class})
public class KStreamBinderConfiguration {

	@Bean
	public KafkaTopicProvisioner provisioningProvider(KafkaBinderConfigurationProperties binderConfigurationProperties,
													KafkaProperties kafkaProperties) {
		return new KafkaTopicProvisioner(binderConfigurationProperties, kafkaProperties);
	}

	@Bean
	public KStreamBinder kStreamBinder(KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
									KafkaTopicProvisioner kafkaTopicProvisioner,
									KafkaStreamsMessageConversionDelegate KafkaStreamsMessageConversionDelegate,
									KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue,
									KeyValueSerdeResolver keyValueSerdeResolver,
									KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties,
									@Qualifier("kafkaStreamsDlqDispatchers") Map<String, KafkaStreamsDlqDispatch> kafkaStreamsDlqDispatchers) {
		KStreamBinder kStreamBinder = new KStreamBinder(binderConfigurationProperties, kafkaTopicProvisioner,
				KafkaStreamsMessageConversionDelegate, KafkaStreamsBindingInformationCatalogue,
				keyValueSerdeResolver, kafkaStreamsDlqDispatchers);
		kStreamBinder.setKafkaStreamsExtendedBindingProperties(kafkaStreamsExtendedBindingProperties);
		return kStreamBinder;
	}

	/**
	 * Registrar for missing beans when there are multiple binders in the application.
	 */
	static class KStreamMissingBeansRegistrar extends KafkaStreamsBinderUtils.KafkaStreamsMissingBeansRegistrar {

		private static final String BEAN_NAME = "outerContext";

		@Override
		public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata,
											BeanDefinitionRegistry registry) {
			super.registerBeanDefinitions(importingClassMetadata, registry);

			if (registry.containsBeanDefinition(BEAN_NAME)) {

				AbstractBeanDefinition converstionDelegateBean = BeanDefinitionBuilder.genericBeanDefinition(MethodInvokingFactoryBean.class)
						.addPropertyReference("targetObject", BEAN_NAME)
						.addPropertyValue("targetMethod", "getBean")
						.addPropertyValue("arguments", KafkaStreamsMessageConversionDelegate.class)
						.getBeanDefinition();

				registry.registerBeanDefinition(KafkaStreamsMessageConversionDelegate.class.getSimpleName(), converstionDelegateBean);

				AbstractBeanDefinition keyValueSerdeResolverBean = BeanDefinitionBuilder.genericBeanDefinition(MethodInvokingFactoryBean.class)
						.addPropertyReference("targetObject", BEAN_NAME)
						.addPropertyValue("targetMethod", "getBean")
						.addPropertyValue("arguments", KeyValueSerdeResolver.class)
						.getBeanDefinition();

				registry.registerBeanDefinition(KeyValueSerdeResolver.class.getSimpleName(), keyValueSerdeResolverBean);

				AbstractBeanDefinition kafkaStreamsExtendedBindingPropertiesBean = BeanDefinitionBuilder.genericBeanDefinition(MethodInvokingFactoryBean.class)
						.addPropertyReference("targetObject", BEAN_NAME)
						.addPropertyValue("targetMethod", "getBean")
						.addPropertyValue("arguments", KafkaStreamsExtendedBindingProperties.class)
						.getBeanDefinition();

				registry.registerBeanDefinition(KafkaStreamsExtendedBindingProperties.class.getSimpleName(), kafkaStreamsExtendedBindingPropertiesBean);
			}
		}
	}

}
