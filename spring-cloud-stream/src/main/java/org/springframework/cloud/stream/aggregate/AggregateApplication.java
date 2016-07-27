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

	private static final String CHANNEL_NAMESPACE_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX + ".channelNamespace";

	private static final String SELF_CONTAINED_APP_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX + ".selfContained";

	public static final String INPUT_CHANNEL_NAME = "input";

	public static final String OUTPUT_CHANNEL_NAME = "output";

	static ConfigurableApplicationContext createParentContext(String[] args, boolean selfContained) {
		SpringApplicationBuilder aggregatorParentConfiguration = new SpringApplicationBuilder();
		aggregatorParentConfiguration
				.sources(AggregatorParentConfiguration.class)
				.web(false)
				.headless(true)
				.properties("spring.jmx.default-domain="
						+ AggregatorParentConfiguration.class.getName(),
						SELF_CONTAINED_APP_PROPERTY_NAME + "=" + selfContained);
		return aggregatorParentConfiguration.run(args);
	}

	static ConfigurableApplicationContext createParentContext(ConfigurableApplicationContext parentContext, String[] args,
			boolean selfContained) {
		SpringApplicationBuilder aggregatorParentConfiguration = new SpringApplicationBuilder();
		aggregatorParentConfiguration
				.sources(AggregatorParentConfiguration.class)
				.web(false)
				.headless(true)
				.properties("spring.jmx.default-domain="
						+ AggregatorParentConfiguration.class.getName(),
						SELF_CONTAINED_APP_PROPERTY_NAME + "=" + selfContained)
				.parent(parentContext);
		return aggregatorParentConfiguration.run(args);
	}

	static String getNamespace(String appClassName, int index) {
		return appClassName + "_" + index;
	}

	protected static SpringApplicationBuilder embedApp(
			ConfigurableApplicationContext parentContext, String namespace,
			Class<?> app) {
		return new SpringApplicationBuilder(app)
				.web(false)
				.main(app)
				.bannerMode(Mode.OFF)
				.properties("spring.jmx.default-domain=" + app)
				.properties(CHANNEL_NAMESPACE_PROPERTY_NAME + "=" + namespace)
				.registerShutdownHook(false)
				.parent(parentContext);
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
