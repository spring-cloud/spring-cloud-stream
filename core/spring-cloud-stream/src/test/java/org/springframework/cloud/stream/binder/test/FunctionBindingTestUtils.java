/*
 * Copyright 2019-2020 the original author or authors.
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

package org.springframework.cloud.stream.binder.test;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistration;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.MessageChannel;

/**
 * A utility class to assist with just-in-time bindings.
 * It is intended for internal framework testing and is NOT for public use. Breaking changes are likely!!!
 *
 * @author Oleg Zhurakousky
 * @since 3.0.2
 *
 */
public class FunctionBindingTestUtils {

	@SuppressWarnings("rawtypes")
	public static void bind(ConfigurableApplicationContext applicationContext, Object function) {
		try {
			Object targetFunction = function;
			if (function instanceof FunctionRegistration) {
				targetFunction = ((FunctionRegistration) function).getTarget();
			}
			String functionName = targetFunction instanceof Function ? "function" : (targetFunction instanceof Consumer ? "consumer" : "supplier");

			System.setProperty("spring.cloud.function.definition", functionName);
			applicationContext.getBeanFactory().registerSingleton(functionName, function);

			Object actualFunction =  ((FunctionInvocationWrapper) applicationContext
					.getBean(FunctionCatalog.class).lookup(functionName)).getTarget();

			InitializingBean functionBindingRegistrar = applicationContext.getBean("functionBindingRegistrar", InitializingBean.class);
			functionBindingRegistrar.afterPropertiesSet();

			BindableProxyFactory bindingProxy = applicationContext.getBean("&" + functionName + "_binding", BindableProxyFactory.class);
			bindingProxy.afterPropertiesSet();

			InitializingBean functionBinder = applicationContext.getBean("functionInitializer", InitializingBean.class);
			functionBinder.afterPropertiesSet();

			BindingServiceProperties bindingProperties = applicationContext.getBean(BindingServiceProperties.class);
			String inputBindingName = functionName + "-in-0";
			String outputBindingName = functionName + "-out-0";
			Map<String, BindingProperties> bindings = bindingProperties.getBindings();
			BindingProperties inputProperties = bindings.get(inputBindingName);
			BindingProperties outputProperties = bindings.get(outputBindingName);
			ConsumerProperties consumerProperties = inputProperties.getConsumer();
			ProducerProperties producerProperties = outputProperties.getProducer();

			TestChannelBinder binder = applicationContext.getBean(TestChannelBinder.class);
			if (actualFunction instanceof Supplier || actualFunction instanceof Function) {
				Binding<MessageChannel> bindProducer = binder.bindProducer(outputProperties.getDestination(),
						applicationContext.getBean(outputBindingName, MessageChannel.class),
						producerProperties == null ? new ProducerProperties() : producerProperties);
				bindProducer.start();
			}
			if (actualFunction instanceof Consumer || actualFunction instanceof Function) {
				Binding<MessageChannel> bindConsumer = binder.bindConsumer(inputProperties.getDestination(), null,
						applicationContext.getBean(inputBindingName, MessageChannel.class),
						consumerProperties == null ? new ConsumerProperties() : consumerProperties);
				bindConsumer.start();
			}
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to bind function", e);
		}
		finally {
			System.clearProperty("spring.cloud.function.definition");
		}
	}
}
