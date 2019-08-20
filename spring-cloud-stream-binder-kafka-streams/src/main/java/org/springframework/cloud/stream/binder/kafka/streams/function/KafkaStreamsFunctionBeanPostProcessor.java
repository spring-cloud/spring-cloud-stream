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
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public class KafkaStreamsFunctionBeanPostProcessor implements InitializingBean, BeanFactoryAware {

	private static final Log LOG = LogFactory.getLog(KafkaStreamsFunctionBeanPostProcessor.class);

	private ConfigurableListableBeanFactory beanFactory;
	private Map<String, ResolvableType> resolvableTypeMap = new TreeMap<>();

	public Map<String, ResolvableType> getResolvableTypes() {
		return this.resolvableTypeMap;
	}

	@Override
	public void afterPropertiesSet() {
		String[] functionNames = this.beanFactory.getBeanNamesForType(Function.class);
		String[] biFunctionNames = this.beanFactory.getBeanNamesForType(BiFunction.class);
		String[] consumerNames = this.beanFactory.getBeanNamesForType(Consumer.class);
		String[] biConsumerNames = this.beanFactory.getBeanNamesForType(BiConsumer.class);

		Stream.concat(
				Stream.concat(Stream.of(functionNames), Stream.of(consumerNames)),
				Stream.concat(Stream.of(biFunctionNames), Stream.of(biConsumerNames)))
				.forEach(this::extractResolvableTypes);
	}

	private void extractResolvableTypes(String key) {
		final Class<?> classObj = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
						this.beanFactory.getBeanDefinition(key))
						.getMetadata().getClassName(),
				ClassUtils.getDefaultClassLoader());
		try {
			Method[] methods = classObj.getMethods();
			Optional<Method> kafkaStreamMethod = Arrays.stream(methods).filter(m -> m.getName().equals(key)).findFirst();
			Method method = kafkaStreamMethod.get();
			ResolvableType resolvableType = ResolvableType.forMethodReturnType(method, classObj);
			resolvableTypeMap.put(key, resolvableType);
		}
		catch (Exception e) {
			LOG.error("Function not found: " + key, e);
		}
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}
}
