/*
 * Copyright 2015-2017 the original author or authors.
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.internal.InternalPropertyNames;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * Utilities for embedding applications in aggregates.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Venil Noronha
 */
abstract class AggregateApplicationUtils {

	public static final String INPUT_BINDING_NAME = "input";

	public static final String OUTPUT_BINDING_NAME = "output";

	static ConfigurableApplicationContext createParentContext(Object[] sources,
			String[] args, final boolean selfContained, boolean webEnvironment,
			boolean headless) {
		SpringApplicationBuilder aggregatorParentConfiguration = new SpringApplicationBuilder();
		aggregatorParentConfiguration.sources(sources).web(webEnvironment)
				.headless(headless)
				.properties("spring.jmx.default-domain="
						+ AggregateApplicationBuilder.ParentConfiguration.class.getName(),
						InternalPropertyNames.SELF_CONTAINED_APP_PROPERTY_NAME + "="
								+ selfContained)
				.properties("management.port=-1");
		return aggregatorParentConfiguration.run(args);
	}

	static String getDefaultNamespace(String appClassName, int index) {
		return appClassName + "_" + index;
	}

	protected static SpringApplicationBuilder embedApp(
			ConfigurableApplicationContext parentContext, String namespace,
			Class<?> app) {
		// Child context needs to enable web MVC configuration and web enabled to obtain
		// the MVC request mapping in the child applications
		return new SpringApplicationBuilder(new Object[] { app, RequestMappingConfiguration.class })
				.web(false)
				.bannerMode(Mode.OFF)
				.properties("spring.jmx.default-domain=" + namespace)
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
				sharedBindingTargetRegistry.register(namespace + "." + OUTPUT_BINDING_NAME,
						sharedChannel);
			}
			i++;
		}
	}

	@Configuration
	@ConditionalOnClass(name = "org.springframework.web.servlet.DispatcherServlet")
	public static class RequestMappingConfiguration {

		@Bean
		@ConditionalOnMissingBean(search = SearchStrategy.CURRENT)
		public RequestMappingHandlerMapping requestMappingHandlerMapping() {
			return new RequestMappingHandlerMapping();
		}
	}

}
