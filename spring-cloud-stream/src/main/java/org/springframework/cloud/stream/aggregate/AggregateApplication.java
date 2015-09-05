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

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;

/**
 * @author Marius Bogoevici
 */
public class AggregateApplication {

	public static final String INPUT_CHANNEL_NAME = "input";

	public static final String OUTPUT_CHANNEL_NAME = "output";

	/**
	 * Supports the aggregation of {@link Source}, {@link Sink} and {@link Processor}
	 * modules by instantiating and binding them directly
	 *
	 * @param parentArgs arguments for the parent (prefixed with '--')
	 * @param modules a list module classes to be aggregated
	 * @param moduleArgs arguments for the modules (prefixed with '--")
	 *
	 * @return the resulting parent context for the aggregate
	 */
	public static ConfigurableApplicationContext run(Class<?>[] modules, String[] parentArgs, String[][] moduleArgs) {
		ConfigurableApplicationContext parentContext = createParentContext(parentArgs != null ? parentArgs
				: new String[0]);
		runEmbedded(parentContext, modules, moduleArgs);
		return parentContext;
	}

	public static ConfigurableApplicationContext run(Class<?>... modules) {
		return run(modules, null, null);
	}

	/**
	 * Embeds a group of modules into an existing parent context
	 *
	 * @param parentContext the parent context
	 * @param modules a list of classes, representing root context definitions for modules
	 * @param args arguments for the modules
	 */
	public static void runEmbedded(ConfigurableApplicationContext parentContext,
			Class<?>[] modules, String[][] args) {
		SharedChannelRegistry bean = parentContext.getBean(SharedChannelRegistry.class);
		prepareSharedChannelRegistry(bean, modules);
		// create child contexts first
		createChildContexts(parentContext, modules, args);
	}

	private static ConfigurableApplicationContext createParentContext(String[] args) {
		SpringApplicationBuilder aggregatorParentConfiguration = new SpringApplicationBuilder();
		aggregatorParentConfiguration
				.sources(AggregatorParentConfiguration.class)
				.web(false)
				.headless(true)
				.properties("spring.jmx.default-domain="
								+ AggregatorParentConfiguration.class.getName());
		return aggregatorParentConfiguration.run(args);
	}

	private static void createChildContexts(ConfigurableApplicationContext parentContext,
			Class<?>[] modules, String args[][]) {
		for (int i = modules.length - 1; i >= 0; i--) {
			String moduleClassName = modules[i].getName();
			embedModule(parentContext, getNamespace(moduleClassName, i), modules[i])
					.run(args != null ? args[i] : new String[0]);
		}
	}

	private static String getNamespace(String moduleClassName, int index) {
		return moduleClassName + "_" + index;
	}

	private static SpringApplicationBuilder embedModule(
			ConfigurableApplicationContext applicationContext, String namespace,
			Class<?> module) {
		return new SpringApplicationBuilder(module)
				.web(false)
				.showBanner(false)
				.properties(BindableProxyFactory.CHANNEL_NAMESPACE_PROPERTY_NAME + "=" + namespace)
				.registerShutdownHook(false)
				.parent(applicationContext);
	}

	private static void prepareSharedChannelRegistry(SharedChannelRegistry sharedChannelRegistry, Class<?>[] modules) {
		DirectChannel sharedChannel = null;
		for (int i = 0; i < modules.length; i++) {
			Class<?> module = modules[i];
			String moduleClassName = module.getName();
			if (i > 0) {
				sharedChannelRegistry.register(getNamespace(moduleClassName, i)
						+ "." + INPUT_CHANNEL_NAME, sharedChannel);
			}
			sharedChannel = new DirectChannel();
			if (i < modules.length - 1) {
				sharedChannelRegistry.register(getNamespace(moduleClassName, i)
						+ "." + OUTPUT_CHANNEL_NAME, sharedChannel);
			}
		}
	}

	/**
	 * Basic configuration for a parent
	 */
	@EnableAutoConfiguration
	@EnableBinding
	public static class AggregatorParentConfiguration {

		@Bean
		@ConditionalOnMissingBean(SharedChannelRegistry.class)
		public SharedChannelRegistry sharedChannelRegistry() {
			return new SharedChannelRegistry();
		}
	}

}
