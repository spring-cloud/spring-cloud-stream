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

import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for KTable binder.
 *
 * @author Soby Chacko
 */
@SuppressWarnings("ALL")
@Configuration
public class KTableBinderConfiguration {

	@Bean
	@ConditionalOnBean(name = "outerContext")
	public static BeanFactoryPostProcessor outerContextBeanFactoryPostProcessor() {
		return (beanFactory) -> {
			// It is safe to call getBean("outerContext") here, because this bean is registered as first
			// and as independent from the parent context.
			ApplicationContext outerContext = (ApplicationContext) beanFactory.getBean("outerContext");
			beanFactory.registerSingleton(KafkaStreamsBinderConfigurationProperties.class.getSimpleName(), outerContext
					.getBean(KafkaStreamsBinderConfigurationProperties.class));
			beanFactory.registerSingleton(KafkaStreamsBindingInformationCatalogue.class.getSimpleName(), outerContext
					.getBean(KafkaStreamsBindingInformationCatalogue.class));
		};
	}

	@Bean
	public KafkaTopicProvisioner provisioningProvider(KafkaBinderConfigurationProperties binderConfigurationProperties,
													KafkaProperties kafkaProperties) {
		return new KafkaTopicProvisioner(binderConfigurationProperties, kafkaProperties);
	}

	@Bean
	public KTableBinder kTableBinder(KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
									KafkaTopicProvisioner kafkaTopicProvisioner,
									@Qualifier("kafkaStreamsDlqDispatchers") Map<String, KafkaStreamsDlqDispatch> kafkaStreamsDlqDispatchers) {
		KTableBinder kStreamBinder = new KTableBinder(binderConfigurationProperties, kafkaTopicProvisioner, kafkaStreamsDlqDispatchers);
		return kStreamBinder;
	}
}
