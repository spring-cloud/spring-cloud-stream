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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.stream.config.BindableProvider;
import org.springframework.core.ResolvableType;
import org.springframework.util.ClassUtils;

/**
 *
 * @author Soby Chacko
 * @since 2.2.0
 *
 */
public class KafkaStreamsFunctionBeanPostProcessor implements InitializingBean, BeanFactoryAware {

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

		Stream.concat(
				Stream.concat(Stream.of(functionNames), Stream.of(consumerNames)), Stream.of(biFunctionNames))
				.forEach(this::extractResolvableTypes);

		BindableProvider bindableProvider =
		clazz -> clazz.isAssignableFrom(KStream.class) || clazz.isAssignableFrom(KTable.class)
				|| clazz.isAssignableFrom(GlobalKTable.class);

		RootBeanDefinition rb = new RootBeanDefinition();
		rb.setInstanceSupplier(() -> bindableProvider);
		rb.setAutowireCandidate(true);
		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;
		registry.registerBeanDefinition("kafkaStreamsBindableProvider", rb);
		//Forcing the bean to be created.
		beanFactory.getBean("kafkaStreamsBindableProvider");
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
