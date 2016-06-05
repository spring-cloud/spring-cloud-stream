/*
 * Copyright 2015-2016 the original author or authors.
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
import java.util.Map.Entry;

import org.springframework.boot.Banner.Mode;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * Class that is responsible for embedding apps using shared channel registry.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Venil Noronha
 */
abstract class AggregateApplication {

	private static final String SPRING_CLOUD_STREAM_INTERNAL_PREFIX = "spring.cloud.stream.internal";

	public static final String CHANNEL_NAMESPACE_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX
			+ ".channelNamespace";

	public static final String INPUT_CHANNEL_NAME = "input";

	public static final String OUTPUT_CHANNEL_NAME = "output";

	/**
	 * Supports the aggregation of {@link Source}, {@link Sink} and {@link Processor} apps
	 * by instantiating and binding them directly
	 *
	 * @param parentArgs arguments for the parent (prefixed with '--')
	 * @param apps a list app classes to be aggregated
	 * @param appArgs arguments for the apps (prefixed with '--")
	 *
	 * @return the resulting parent context for the aggregate
	 */
	static ConfigurableApplicationContext run(Class<?>[] apps, String[] parentArgs, String[][] appArgs) {
		ConfigurableApplicationContext parentContext = createParentContext(
				parentArgs != null ? parentArgs : new String[0]);
		runEmbedded(parentContext, apps, appArgs);
		return parentContext;
	}

	static ConfigurableApplicationContext run(Class<?>... apps) {
		return run(apps, null, null);
	}

	/**
	 * Embeds a group of apps into an existing parent context
	 *
	 * @param parentContext the parent context
	 * @param apps a list of classes, representing root context definitions for apps
	 * @param args arguments for the apps
	 */
	static void runEmbedded(ConfigurableApplicationContext parentContext, Class<?>[] apps, String[][] args) {
		SharedChannelRegistry bean = parentContext.getBean(SharedChannelRegistry.class);
		prepareSharedChannelRegistry(bean, apps);
		// create child contexts first
		createChildContexts(parentContext, apps, args);
	}

	static ConfigurableApplicationContext createParentContext(String[] args) {
		SpringApplicationBuilder aggregatorParentConfiguration = new SpringApplicationBuilder();
		aggregatorParentConfiguration.sources(AggregatorParentConfiguration.class).web(false).headless(true)
				.properties("spring.jmx.default-domain=" + AggregatorParentConfiguration.class.getName());
		return aggregatorParentConfiguration.run(args);
	}

	static void createChildContexts(ConfigurableApplicationContext parentContext, Class<?>[] apps, String[][] args) {
		for (int i = apps.length - 1; i >= 0; i--) {
			String appClassName = apps[i].getName();
			embedApp(parentContext, getNamespace(appClassName, i), apps[i]).run(args != null ? args[i] : new String[0]);
		}
	}

	static String getNamespace(String appClassName, int index) {
		return appClassName + "_" + index;
	}

	protected static SpringApplicationBuilder embedApp(ConfigurableApplicationContext applicationContext,
			String namespace, Class<?> app) {
		return new SpringApplicationBuilder(app).web(false).main(app).bannerMode(Mode.OFF)
				.properties("spring.jmx.default-domain=" + app)
				.properties(CHANNEL_NAMESPACE_PROPERTY_NAME + "=" + namespace).registerShutdownHook(false)
				.parent(applicationContext);
	}

	static void prepareSharedChannelRegistry(SharedChannelRegistry sharedChannelRegistry, Class<?>[] apps) {
		LinkedHashMap<Class<?>, String> appsToRegister = new LinkedHashMap<>();
		for (int i = apps.length - 1; i >= 0; i--) {
			String appClassName = apps[i].getName();
			appsToRegister.put(apps[i], getNamespace(appClassName, i));
		}
		prepareSharedChannelRegistry(sharedChannelRegistry, appsToRegister);
	}

	static void prepareSharedChannelRegistry(SharedChannelRegistry sharedChannelRegistry,
			LinkedHashMap<Class<?>, String> appsWithNamespace) {
		int i = 0;
		SubscribableChannel sharedChannel = null;
		for (Entry<Class<?>, String> appEntry : appsWithNamespace.entrySet()) {
			String namespace = appEntry.getValue();
			if (i > 0) {
				sharedChannelRegistry.register(namespace + "." + INPUT_CHANNEL_NAME, sharedChannel);
			}
			sharedChannel = new DirectChannel();
			if (i < appsWithNamespace.size() - 1) {
				sharedChannelRegistry.register(namespace + "." + OUTPUT_CHANNEL_NAME, sharedChannel);
			}
			i++;
		}
	}

}
