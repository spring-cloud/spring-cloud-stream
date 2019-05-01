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
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.ResolvableType;
import org.springframework.util.ClassUtils;

/**
 *
 * @author Soby Chacko
 * @since 2.2.0
 *
 */
class KafkaStreamsFunctionBeanPostProcessor implements InitializingBean, BeanFactoryAware {

	private ConfigurableListableBeanFactory beanFactory;
	private Map<String, ResolvableType> resolvableTypeMap = new TreeMap<>();

	public Map<String, ResolvableType> getResolvableTypes() {
		return this.resolvableTypeMap;
	}

	@Override
	public void afterPropertiesSet() {

		String[] functionNames = this.beanFactory.getBeanNamesForType(Function.class);
		String[] consumerNames = this.beanFactory.getBeanNamesForType(Consumer.class);

		Stream.concat(Stream.of(functionNames), Stream.of(consumerNames)).forEach(this::extractResolvableTypes);
	}

	private void extractResolvableTypes(String key) {
		final Class<?> classObj = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
						this.beanFactory.getBeanDefinition(key))
						.getMetadata().getClassName(),
				ClassUtils.getDefaultClassLoader());
		try {
			Method method = classObj.getMethod(key);
			ResolvableType resolvableType = ResolvableType.forMethodReturnType(method, classObj);
			resolvableTypeMap.put(key, resolvableType);
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
