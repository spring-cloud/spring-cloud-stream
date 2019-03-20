/*
 * Copyright 2019-2019 the original author or authors.
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

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.util.ClassUtils;

public class KafkaStreamsFunctionBeanPostProcessor implements InitializingBean {

	private final KafkaStreamsFunctionProperties kafkaStreamsFunctionProperties;
	private final ConfigurableApplicationContext context;
	private ResolvableType resolvableType;

	public KafkaStreamsFunctionBeanPostProcessor(KafkaStreamsFunctionProperties properties,
												ConfigurableApplicationContext context) {
		this.kafkaStreamsFunctionProperties = properties;
		this.context = context;
	}

	public ResolvableType getResolvableType() {
		return this.resolvableType;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		final Class<?> classObj = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
						context.getBeanFactory().getBeanDefinition(kafkaStreamsFunctionProperties.getDefinition()))
						.getMetadata().getClassName(),
				ClassUtils.getDefaultClassLoader());

		try {
			Method method = classObj.getMethod(this.kafkaStreamsFunctionProperties.getDefinition(), null);
			this.resolvableType = ResolvableType.forMethodReturnType(method, classObj);
		}
		catch (NoSuchMethodException e) {
			//ignore
		}
	}
}
