/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.aggregate;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.springframework.boot.Banner.Mode;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.internal.InternalPropertyNames;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * Utilities for embedding applications in aggregates.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Venil Noronha
 * @author Janne Valkealahti
 */
abstract class AggregateApplicationUtils {

	public static final String INPUT_BINDING_NAME = "input";

	public static final String OUTPUT_BINDING_NAME = "output";

	static ConfigurableApplicationContext createParentContext(Class<?>[] sources,
			String[] args, final boolean selfContained, boolean webEnvironment,
			boolean headless) {
		SpringApplicationBuilder aggregatorParentConfiguration = new SpringApplicationBuilder();
		aggregatorParentConfiguration.sources(sources).web(WebApplicationType.NONE)
				.headless(headless)
				.properties("spring.jmx.default-domain="
						+ AggregateApplicationBuilder.ParentConfiguration.class.getName(),
						InternalPropertyNames.SELF_CONTAINED_APP_PROPERTY_NAME + "="
								+ selfContained);
		return aggregatorParentConfiguration.run(args);
	}

	static String getDefaultNamespace(String appClassName, int index) {
		return appClassName + "-" + index;
	}

	protected static SpringApplicationBuilder embedApp(
			ConfigurableApplicationContext parentContext, String namespace,
			Class<?> app) {
		return new SpringApplicationBuilder(app).web(WebApplicationType.NONE).main(app)
				.bannerMode(Mode.OFF).properties("spring.jmx.default-domain=" + namespace)
				.properties(
						InternalPropertyNames.NAMESPACE_PROPERTY_NAME + "=" + namespace)
				.registerShutdownHook(false).parent(parentContext);
	}

	static void prepareSharedBindingTargetRegistry(
			SharedBindingTargetRegistry sharedBindingTargetRegistry,
			LinkedHashMap<Class<?>, String> appsWithNamespace) {
		int i = 0;
		SubscribableChannel sharedChannel = null;
		for (Entry<Class<?>, String> appEntry : appsWithNamespace.entrySet()) {
			String namespace = appEntry.getValue();
			if (i > 0) {
				sharedBindingTargetRegistry.register(namespace + "." + INPUT_BINDING_NAME,
						sharedChannel);
			}
			sharedChannel = new DirectChannel();
			if (i < appsWithNamespace.size() - 1) {
				sharedBindingTargetRegistry
						.register(namespace + "." + OUTPUT_BINDING_NAME, sharedChannel);
			}
			i++;
		}
	}

}
