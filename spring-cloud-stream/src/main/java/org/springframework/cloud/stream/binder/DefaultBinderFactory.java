/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.OrderedHealthAggregator;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default {@link BinderFactory} implementation.
 *
 * @author Marius Bogoevici
 */
public class DefaultBinderFactory<T> implements BinderFactory<T>, DisposableBean, ApplicationContextAware {

	private final Map<String, BinderConfiguration> binderConfigurations;

	private final Map<String, BinderInstanceHolder<T>> binderInstanceCache = new HashMap<>();

	private volatile ConfigurableApplicationContext context;

	private volatile String defaultBinder;

	private volatile CompositeHealthIndicator bindersHealthIndicator;

	public DefaultBinderFactory(Map<String, BinderConfiguration> binderConfigurations) {
		this.binderConfigurations = new HashMap<>(binderConfigurations);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		Assert.isInstanceOf(ConfigurableApplicationContext.class, applicationContext);
		this.context = (ConfigurableApplicationContext) applicationContext;
	}

	@Autowired(required = false)
	@Qualifier("bindersHealthIndicator")
	public void setBindersHealthIndicator(CompositeHealthIndicator bindersHealthIndicator) {
		this.bindersHealthIndicator = bindersHealthIndicator;
	}

	public void setDefaultBinder(String defaultBinder) {
		this.defaultBinder = defaultBinder;
	}

	@Override
	public void destroy() throws Exception {
		for (Map.Entry<String, BinderInstanceHolder<T>> entry : this.binderInstanceCache.entrySet()) {
			BinderInstanceHolder<T> binderInstanceHolder = entry.getValue();
			binderInstanceHolder.getBinderContext().close();
		}
	}

	@Override
	public synchronized Binder<T,?,?> getBinder(String name) {
		String configurationName;
		// Fall back to a default if no argument is provided
		if (StringUtils.isEmpty(name)) {
			if (this.binderConfigurations.size() == 0) {
				throw new IllegalStateException("A default binder has been requested, but there there is no binder available");
			}
			else if (this.binderConfigurations.size() == 1) {
				configurationName = this.binderConfigurations.keySet().iterator().next();
			}
			else {
				if (StringUtils.hasText(this.defaultBinder)) {
					configurationName = this.defaultBinder;
				}
				else {
					throw new IllegalStateException(
							"A default binder has been requested, but there is more than one binder available: "
									+ StringUtils.collectionToCommaDelimitedString(this.binderConfigurations.keySet()) + ", and"
									+ " no default binder has been set.");
				}
			}
		} else {
			configurationName = name;
		}
		if (!this.binderInstanceCache.containsKey(configurationName)) {
			BinderConfiguration binderConfiguration = this.binderConfigurations.get(configurationName);
			if (binderConfiguration == null) {
				throw new IllegalStateException("Unknown binder configuration: " + configurationName);
			}
			Properties binderProperties = binderConfiguration.getProperties();
			// Convert all properties to arguments, so that they receive maximum precedence
			ArrayList<String> args = new ArrayList<>();
			for (Map.Entry<Object, Object> property : binderProperties.entrySet()) {
				args.add(String.format("--%s=%s",property.getKey(),property.getValue()));
			}
			// Initialize the domain with a unique name based on the bootstrapping context setting
			ConfigurableEnvironment environment = context != null ? context.getEnvironment() : null;
			String defaultDomain = environment != null ? environment.getProperty("spring.jmx.default-domain") : null;
			if (defaultDomain == null) {
				defaultDomain = "";
			}
			else {
				defaultDomain += ".";
			}
			args.add("--spring.jmx.default-domain=" + defaultDomain + "binder." + configurationName);
			List<Class<?>> configurationClasses =
					new ArrayList<>(Arrays.asList(binderConfiguration.getBinderType().getConfigurationClasses()));
			SpringApplicationBuilder springApplicationBuilder =
					new SpringApplicationBuilder()
							.sources(configurationClasses.toArray(new Class<?>[]{}))
							.bannerMode(Mode.OFF)
							.web(false);
			// If the environment is not customized and a main context is available, we will set the latter as parent.
			// This ensures that the defaults and user-defined customizations (e.g. custom connection factory beans)
			// are propagated to the binder context. If the environment is customized, then the binder context should
			// not inherit any beans from the parent
			boolean useApplicationContextAsParent = binderProperties.isEmpty() && context != null;
			if (useApplicationContextAsParent) {
				springApplicationBuilder.parent(context);
			}
			if (useApplicationContextAsParent || (environment != null && binderConfiguration.isInheritEnvironment())) {
				StandardEnvironment binderEnvironment = new StandardEnvironment();
				binderEnvironment.merge(environment);
				springApplicationBuilder.environment(binderEnvironment);
			}
			ConfigurableApplicationContext binderProducingContext =
					springApplicationBuilder.run(args.toArray(new String[args.size()]));
			@SuppressWarnings("unchecked")
			Binder<T,?,?> binder = binderProducingContext.getBean(Binder.class);
			if (bindersHealthIndicator != null) {
				OrderedHealthAggregator healthAggregator = new OrderedHealthAggregator();
				Map<String, HealthIndicator> indicators = binderProducingContext.getBeansOfType(HealthIndicator.class);
				// if there are no health indicators in the child context, we just mark the binder's health as unknown
				// this can happen due to the fact that configuration is inherited
				HealthIndicator binderHealthIndicator =
						indicators.isEmpty() ? new DefaultHealthIndicator() : new CompositeHealthIndicator(healthAggregator, indicators);
				bindersHealthIndicator.addHealthIndicator(configurationName, binderHealthIndicator);
			}
			this.binderInstanceCache.put(configurationName, new BinderInstanceHolder<>(binder, binderProducingContext));
		}
		return this.binderInstanceCache.get(configurationName).getBinderInstance();
	}

	/**
	 * Utility class for storing {@link Binder} instances, along with their associated contexts.
	 *
	 * @param <T>
	 */
	private static class BinderInstanceHolder<T> {

		private final Binder<T,?,?> binderInstance;

		private final ConfigurableApplicationContext binderContext;

		public BinderInstanceHolder(Binder<T,?,?> binderInstance, ConfigurableApplicationContext binderContext) {
			this.binderInstance = binderInstance;
			this.binderContext = binderContext;
		}

		public Binder<T,?,?> getBinderInstance() {
			return this.binderInstance;
		}

		public ConfigurableApplicationContext getBinderContext() {
			return this.binderContext;
		}
	}

	private static class DefaultHealthIndicator extends AbstractHealthIndicator {

		@Override
		protected void doHealthCheck(Health.Builder builder) throws Exception {
			builder.unknown();
		}
	}
}
