/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.lang.reflect.Method;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.util.ClassUtils;

public class KafkaStreamsFunctionBeanPostProcessor implements BeanPostProcessor {

	private final String functionName;
	private final ConfigurableApplicationContext context;
	private ResolvableType resolvableType;

	public KafkaStreamsFunctionBeanPostProcessor(String functionName, ConfigurableApplicationContext context) {
		this.functionName = functionName;
		this.context = context;
	}

	@Override
	public final Object postProcessAfterInitialization(Object bean, final String beanName) throws BeansException {

		if (beanName.equals(this.functionName)) {
			final Class<?> classObj = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
							this.context.getBeanFactory().getBeanDefinition(beanName)).getMetadata().getClassName(),
					ClassUtils.getDefaultClassLoader());

			try {
				Method method = classObj.getMethod(this.functionName, null);
				resolvableType = ResolvableType.forMethodReturnType(method, classObj);
			} catch (NoSuchMethodException e) {
				//ignore
			}
		}
		return bean;
	}

	public ResolvableType getResolvableType() {
		return this.resolvableType;
	}
}
