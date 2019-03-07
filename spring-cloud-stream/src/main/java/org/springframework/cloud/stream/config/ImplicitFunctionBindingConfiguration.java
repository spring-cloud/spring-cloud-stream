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

package org.springframework.cloud.stream.config;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binding.BindingBeanDefinitionRegistryUtils;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 *
 * @author Oleg Zhurakousky
 * @since 2.2
 *
 */
class ImplicitFunctionBindingConfiguration {

	@Bean
	public BeanFactoryPostProcessor implicitFunctionBinder(Environment environment,
			@Nullable FunctionRegistry functionCatalog, @Nullable FunctionInspector inspector) {
		return new BeanFactoryPostProcessor() {
			@Override
			public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
				if (functionCatalog != null && ObjectUtils.isEmpty(beanFactory.getBeanNamesForAnnotation(EnableBinding.class))) {
					BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;
					String name = determineFunctionName(functionCatalog, environment);
					if (StringUtils.hasText(name)) {
						Object definedFunction = functionCatalog.lookup(name);
						Class<?> inputType = inspector.getInputType(definedFunction);
						Class<?> outputType = inspector.getOutputType(definedFunction);
						if (Void.class.isAssignableFrom(outputType)) {
							bind(Sink.class, registry);
						}
						else if (Void.class.isAssignableFrom(inputType)) {
							bind(Source.class, registry);
						}
						else {
							bind(Processor.class, registry);
						}
					}
				}
			}
		};
	}

	private void bind(Class<?> type, BeanDefinitionRegistry registry) {
		if (!registry.containsBeanDefinition(type.getName())) {
			BindingBeanDefinitionRegistryUtils.registerBindingTargetBeanDefinitions(type, type.getName(), registry);
			BindingBeanDefinitionRegistryUtils.registerBindingTargetsQualifiedBeanDefinitions(type, type, registry);
		}
	}

	private String determineFunctionName(FunctionCatalog catalog, Environment environment) {
		String name = environment.getProperty("spring.cloud.stream.function.definition");
		if (!StringUtils.hasText(name)) {
			name = catalog.size() == 1
					? catalog.getNames(Function.class).stream().findFirst()
							.orElseGet(() -> catalog.getNames(Consumer.class).stream().findFirst()
									.orElseGet(() -> catalog.getNames(Supplier.class).stream().findFirst().orElse(null)))
							: null;
			if (StringUtils.hasText(name)) {
				((StandardEnvironment) environment).getSystemProperties()
						.putIfAbsent("spring.cloud.stream.function.definition", name);
			}
		}

		return name;
	}
}
