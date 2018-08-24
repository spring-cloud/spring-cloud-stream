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

package org.springframework.cloud.stream.function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionType;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.binding.BindingBeanDefinitionRegistryUtils;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.ClassUtils;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @since 2.1
 */
@Configuration
@ConditionalOnProperty("spring.cloud.stream.function.name")
public class FunctionConfiguration {

	@Autowired(required=false)
	private Source source;

	@Autowired(required=false)
	private Processor processor;

	@Autowired(required=false)
	private Sink sink;

	@Autowired
	private ConfigurableListableBeanFactory registry;

	@Bean
	public IntegrationFlowFunctionSupport functionSupport(FunctionCatalogWrapper functionCatalog,
			FunctionInspector functionInspector, CompositeMessageConverterFactory messageConverterFactory,
			FunctionProperties functionProperties) {

		return new IntegrationFlowFunctionSupport(functionCatalog, functionInspector, messageConverterFactory,
				functionProperties);
	}

	@Bean
	public FunctionCatalogWrapper functionCatalogWrapper(FunctionCatalog catalog) {
		return new FunctionCatalogWrapper(catalog);
	}


	@ConditionalOnProperty("spring.cloud.stream.function.name")
	@ConditionalOnMissingBean
	@Bean
	public IntegrationFlow foo(IntegrationFlowFunctionSupport functionSupport) {
		if (processor != null) {
			return functionSupport.integrationFlowForFunction(processor.input(), processor.output()).get();
		}
		else if (sink != null) {
			return functionSupport.integrationFlowForFunction(sink.input(), null).get();
		}
		else if (source != null) {
			return functionSupport.integrationFlowFromNamedSupplier().channel(this.source.output()).get();
		}

		FunctionType ft = functionSupport.getCurrentFunctionType();
		BindingBeanDefinitionRegistryUtils.registerBindingTargetBeanDefinitions(Sink.class,
				Sink.class.getName(), (BeanDefinitionRegistry) registry);
		BindingBeanDefinitionRegistryUtils.registerBindingTargetsQualifiedBeanDefinitions(
				ClassUtils.resolveClassName(this.getClass().getName(), null), Sink.class,
				(BeanDefinitionRegistry) registry);
		return functionSupport.integrationFlowForFunction(registry.getBean("input", SubscribableChannel.class), null).get();
		//throw new UnsupportedOperationException("Not yet supotrted");
	}
}
