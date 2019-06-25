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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsFunctionProcessor;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * @author Soby Chacko
 * @since 2.2.0
 */
@Configuration
@EnableConfigurationProperties(StreamFunctionProperties.class)
@AutoConfigureBefore(BinderFactoryAutoConfiguration.class)
public class KafkaStreamsFunctionAutoConfiguration {

	@Bean
	@Conditional(FunctionDetectorCondition.class)
	public KafkaStreamsFunctionProcessorInvoker kafkaStreamsFunctionProcessorInvoker(
																					KafkaStreamsFunctionBeanPostProcessor kafkaStreamsFunctionBeanPostProcessor,
																					KafkaStreamsFunctionProcessor kafkaStreamsFunctionProcessor) {
		return new KafkaStreamsFunctionProcessorInvoker(kafkaStreamsFunctionBeanPostProcessor.getResolvableTypes(),
				kafkaStreamsFunctionProcessor);
	}

	@Bean
	@Conditional(FunctionDetectorCondition.class)
	public KafkaStreamsFunctionBeanPostProcessor kafkaStreamsFunctionBeanPostProcessor() {
		return new KafkaStreamsFunctionBeanPostProcessor();
	}

	@Bean
	@Conditional(FunctionDetectorCondition.class)
	public BeanFactoryPostProcessor implicitFunctionBinderhello(KafkaStreamsFunctionBeanPostProcessor kafkaStreamsFunctionBeanPostProcessor) {
		return new BeanFactoryPostProcessor() {
			@Override
			public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
				BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;

				for (String s : kafkaStreamsFunctionBeanPostProcessor.getResolvableTypes().keySet()) {
					RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(
							KafkaStreamsBindableProxyFactory.class);
					rootBeanDefinition.getConstructorArgumentValues()
							.addGenericArgumentValue(kafkaStreamsFunctionBeanPostProcessor.getResolvableTypes().get(s));
					rootBeanDefinition.getConstructorArgumentValues()
							.addGenericArgumentValue(s);
					registry.registerBeanDefinition("kafkaStreamsBindableProxyFactory", rootBeanDefinition);
				}
			}
		};
	}
}
