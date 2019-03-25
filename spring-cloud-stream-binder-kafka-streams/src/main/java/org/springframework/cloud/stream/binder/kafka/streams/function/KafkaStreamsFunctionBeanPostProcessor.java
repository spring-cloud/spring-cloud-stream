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

import java.lang.reflect.Method;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.core.ResolvableType;
import org.springframework.util.ClassUtils;

/**
 *
 * @author Soby Chacko
 * @since 2.1.0
 *
 */
class KafkaStreamsFunctionBeanPostProcessor implements InitializingBean, BeanFactoryAware {

	private final StreamFunctionProperties kafkaStreamsFunctionProperties;
	private ConfigurableListableBeanFactory beanFactory;
	private ResolvableType resolvableType;

	KafkaStreamsFunctionBeanPostProcessor(StreamFunctionProperties properties) {
		this.kafkaStreamsFunctionProperties = properties;
	}

	public ResolvableType getResolvableType() {
		return this.resolvableType;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		final Class<?> classObj = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
						this.beanFactory.getBeanDefinition(kafkaStreamsFunctionProperties.getDefinition()))
						.getMetadata().getClassName(),
				ClassUtils.getDefaultClassLoader());

		try {
			Method method = classObj.getMethod(this.kafkaStreamsFunctionProperties.getDefinition());
			this.resolvableType = ResolvableType.forMethodReturnType(method, classObj);
		}
		catch (NoSuchMethodException e) {
			//ignore
		}
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}
}
