/*
 * Copyright 2018 the original author or authors.
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

import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author Soby Chacko
 * @since 2.1.0
 */
@Configuration
@Import(GlobalKTableBinderConfiguration.Registrar.class)
public class GlobalKTableBinderConfiguration {

	static class Registrar implements ImportBeanDefinitionRegistrar {

		private static final String BEAN_NAME = "outerContext";

		@Override
		public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata,
											BeanDefinitionRegistry registry) {
			if (registry.containsBeanDefinition(BEAN_NAME)) {

				AbstractBeanDefinition configBean = BeanDefinitionBuilder.genericBeanDefinition(MethodInvokingFactoryBean.class)
						.addPropertyReference("targetObject", BEAN_NAME)
						.addPropertyValue("targetMethod", "getBean")
						.addPropertyValue("arguments", KafkaStreamsBinderConfigurationProperties.class)
						.getBeanDefinition();

				registry.registerBeanDefinition(KafkaStreamsBinderConfigurationProperties.class.getSimpleName(), configBean);

				AbstractBeanDefinition catalogueBean = BeanDefinitionBuilder.genericBeanDefinition(MethodInvokingFactoryBean.class)
						.addPropertyReference("targetObject", BEAN_NAME)
						.addPropertyValue("targetMethod", "getBean")
						.addPropertyValue("arguments", KafkaStreamsBindingInformationCatalogue.class)
						.getBeanDefinition();

				registry.registerBeanDefinition(KafkaStreamsBindingInformationCatalogue.class.getSimpleName(), catalogueBean);
			}
		}
	}

	@Bean
	public KafkaTopicProvisioner provisioningProvider(KafkaBinderConfigurationProperties binderConfigurationProperties,
													KafkaProperties kafkaProperties) {
		return new KafkaTopicProvisioner(binderConfigurationProperties, kafkaProperties);
	}

	@Bean
	public GlobalKTableBinder GlobalKTableBinder(KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
									KafkaTopicProvisioner kafkaTopicProvisioner,
									KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue) {
		return new GlobalKTableBinder(binderConfigurationProperties, kafkaTopicProvisioner,
				KafkaStreamsBindingInformationCatalogue);
	}
}
