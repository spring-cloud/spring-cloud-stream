/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.aggregate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableModule;
import org.springframework.cloud.stream.annotation.Processor;
import org.springframework.cloud.stream.annotation.Sink;
import org.springframework.cloud.stream.annotation.Source;
import org.springframework.cloud.stream.binding.BindableContextWrapper;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.binding.BindingUtils;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;

/**
 * @author Marius Bogoevici
 */
public class ModuleAggregationUtils {

	private static final Log log = LogFactory.getLog(ModuleAggregationUtils.class);

	/**
	 * Supports the aggregation of {@link Source}, {@link Sink} and {@link Processor} modules by
	 * binding them directly
	 *
	 * @param modules
	 * @param moduleArgs
	 * @return
	 */
	public static ConfigurableApplicationContext runAggregated(String[] parentArgs,Class<?>[] modules, String[][] moduleArgs) {
		ConfigurableApplicationContext parentContext = createParentContext(parentArgs);
		runEmbedded(parentContext, modules, moduleArgs);
		return parentContext;
	}

	public static ConfigurableApplicationContext runAggregated(Class<?>... modules) {
		return runAggregated(null, modules, null);
	}

	public static void runEmbedded(ConfigurableApplicationContext parentContext, Class<?>[] modules,
																 String[][] args) {
		SharedChannelRegistry bean = parentContext.getBean(SharedChannelRegistry.class);
		prepareSharedChannelRegistry(bean, modules);
		LinkedHashMap<String, SpringApplicationBuilder> children = createChildContexts(parentContext, modules, args);
		for (Map.Entry<String, SpringApplicationBuilder> childContextEntry : children.entrySet()) {
			ConfigurableApplicationContext childContext = childContextEntry.getValue().run();
			parentContext.getBeanFactory().registerSingleton(childContextEntry.getKey(),
					new BindableContextWrapper(childContext));
			childContext.setParent(parentContext);
		}
		BindingUtils.bindAll(parentContext);
	}

	private static ConfigurableApplicationContext createParentContext(String[] args) {
		SpringApplicationBuilder aggregatorParentConfiguration
				= new SpringApplicationBuilder();
		aggregatorParentConfiguration.sources(AggregatorParentConfiguration.class)
				.web(false)
				.headless(true)
				.properties(args != null ? args : new String[0])
				.listeners(new UnbindOnCloseApplicationListener());
		ConfigurableApplicationContext applicationContext =
				aggregatorParentConfiguration.run("--spring.jmx.default-domain=" + UUID.randomUUID().toString().replace("-", ""));
		return applicationContext;
	}

	private static LinkedHashMap<String, SpringApplicationBuilder> createChildContexts(ConfigurableApplicationContext applicationContext, Class<?>[] modules,
																																										 String args[][]) {
		LinkedHashMap<String, SpringApplicationBuilder> children = new LinkedHashMap<>();
		for (int i = modules.length - 1; i >= 0; i--) {
			SpringApplicationBuilder childModule = embedModule(applicationContext, modules[i], args!=null ? args[i] : null);
			children.put(modules[i].getName(), childModule);
		}
		return children;
	}

	/**
	 * Allows embedding a module inside a context application. Invokers may supply information about channels
	 * that will be bound through a {@link SharedChannelRegistry} bean deployed in the parent.
	 *
	 * @param args
	 */

	public static SpringApplicationBuilder embedModule(ConfigurableApplicationContext applicationContext, Class<?> module, String[] args) {
		return new SpringApplicationBuilder(module)
				.web(false)
				.showBanner(false)
				.properties(BindableProxyFactory.CHANNEL_NAMESPACE_PROPERTY_NAME + "=" + module.getName())
				.properties(args != null ? args : new String[0])
				.registerShutdownHook(false)
				.parent(applicationContext);
	}

	private static void prepareSharedChannelRegistry(SharedChannelRegistry sharedChannelRegistry, Class<?>[] modules) {
		DirectChannel sharedChannel = null;
		for (int i = 0; i < modules.length; i++) {
			Class<?> module = modules[i];
			if (i > 0) {
				sharedChannelRegistry.putSharedChannel(module.getName() + ".input", sharedChannel);
			}
			sharedChannel = new DirectChannel();
			if (i < modules.length - 1) {
				sharedChannelRegistry.putSharedChannel(module.getName() + ".output", sharedChannel);
			}
		}
	}

	@EnableAutoConfiguration
	@EnableModule
	public static class AggregatorParentConfiguration {

		@Bean
		@ConditionalOnMissingBean(SharedChannelRegistry.class)
		public SharedChannelRegistry sharedChannelRegistry() {
			return new SharedChannelRegistry();
		}
	}

}
