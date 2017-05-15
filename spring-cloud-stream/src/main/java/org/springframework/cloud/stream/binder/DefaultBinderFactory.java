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

package org.springframework.cloud.stream.binder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.reflection.GenericsUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default {@link BinderFactory} implementation.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class DefaultBinderFactory implements BinderFactory, DisposableBean, ApplicationContextAware {

	private final Map<String, BinderConfiguration> binderConfigurations;

	private final Map<String, BinderInstanceHolder> binderInstanceCache = new HashMap<>();

	private volatile ConfigurableApplicationContext context;

	private Map<String, String> defaultBinderForBindingTargetType = new HashMap<>();

	private Collection<Listener> listeners;

	private volatile String defaultBinder;

	public DefaultBinderFactory(Map<String, BinderConfiguration> binderConfigurations) {
		this.binderConfigurations = new HashMap<>(binderConfigurations);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		Assert.isInstanceOf(ConfigurableApplicationContext.class, applicationContext);
		this.context = (ConfigurableApplicationContext) applicationContext;
	}

	public void setDefaultBinder(String defaultBinder) {
		this.defaultBinder = defaultBinder;
	}

	public void setListeners(Collection<Listener> listeners) {
		this.listeners = listeners;
	}

	@Override
	public void destroy() throws Exception {
		for (Map.Entry<String, BinderInstanceHolder> entry : this.binderInstanceCache.entrySet()) {
			BinderInstanceHolder binderInstanceHolder = entry.getValue();
			binderInstanceHolder.getBinderContext().close();
		}
		this.defaultBinderForBindingTargetType.clear();
	}

	@Override
	public synchronized <T> Binder<T, ?, ?> getBinder(String name, Class<? extends T> bindingTargetType) {
		String configurationName;
		// Fall back to a default if no argument is provided
		if (StringUtils.isEmpty(name)) {
			if (this.binderConfigurations.size() == 0) {
				throw new IllegalStateException(
						"A default binder has been requested, but there there is no binder available");
			}
			else if (!StringUtils.hasText(this.defaultBinder)) {
				Set<String> defaultCandidateConfigurations = new HashSet<>();
				for (Map.Entry<String, BinderConfiguration> binderConfigurationEntry : this.binderConfigurations
						.entrySet()) {
					if (binderConfigurationEntry.getValue().isDefaultCandidate()) {
						defaultCandidateConfigurations.add(binderConfigurationEntry.getKey());
					}
				}
				if (defaultCandidateConfigurations.size() == 1) {
					configurationName = defaultCandidateConfigurations.iterator().next();
					this.defaultBinderForBindingTargetType.put(bindingTargetType.getName(), configurationName);
				}
				else {
					if (defaultCandidateConfigurations.size() > 1) {
						List<String> candidatesForBindableType = new ArrayList<>();
						for (String defaultCandidateConfiguration : defaultCandidateConfigurations) {
							Binder<Object, ?, ?> binderInstance = getBinderInstance(defaultCandidateConfiguration);
							Class<?> binderType = GenericsUtils.getParameterType(binderInstance.getClass(),
									Binder.class, 0);
							if (binderType.isAssignableFrom(bindingTargetType)) {
								candidatesForBindableType.add(defaultCandidateConfiguration);
							}
						}
						if (candidatesForBindableType.size() == 1) {
							configurationName = candidatesForBindableType.iterator().next();
							this.defaultBinderForBindingTargetType.put(bindingTargetType.getName(), configurationName);
						}
						else if (candidatesForBindableType.size() > 1) {
							throw new IllegalStateException(
									"A default binder has been requested, but there is more than one binder available for '"
											+ bindingTargetType.getName() + "' : "
											+ StringUtils.collectionToCommaDelimitedString(candidatesForBindableType)
											+ ", and no default binder has been set.");
						}
						else {
							throw new IllegalStateException("A default binder has been requested, but none of the "
									+ "registered binders can bind a '" + bindingTargetType + "': "
									+ StringUtils.collectionToCommaDelimitedString(defaultCandidateConfigurations));
						}
					}
					else {
						throw new IllegalArgumentException(
								"A default binder has been requested, but there is no default available");
					}
				}
			}
			else {
				configurationName = this.defaultBinder;
			}
		}
		else {
			configurationName = name;
		}
		Binder<T, ?, ?> binderInstance = getBinderInstance(configurationName);
		if (!(GenericsUtils.getParameterType(binderInstance.getClass(), Binder.class, 0)
				.isAssignableFrom(bindingTargetType))) {
			throw new IllegalStateException(
					"The binder '" + configurationName + "' cannot bind a " + bindingTargetType.getName());
		}
		return binderInstance;
	}

	private <T> Binder<T, ?, ?> getBinderInstance(String configurationName) {
		if (!this.binderInstanceCache.containsKey(configurationName)) {
			BinderConfiguration binderConfiguration = this.binderConfigurations.get(configurationName);
			if (binderConfiguration == null) {
				throw new IllegalStateException("Unknown binder configuration: " + configurationName);
			}
			Properties binderProperties = binderConfiguration.getProperties();
			// Convert all properties to arguments, so that they receive maximum
			// precedence
			ArrayList<String> args = new ArrayList<>();
			for (Map.Entry<Object, Object> property : binderProperties.entrySet()) {
				args.add(String.format("--%s=%s", property.getKey(), property.getValue()));
			}
			// Initialize the domain with a unique name based on the bootstrapping context
			// setting
			ConfigurableEnvironment environment = this.context != null ? this.context.getEnvironment() : null;
			String defaultDomain = environment != null ? environment.getProperty("spring.jmx.default-domain") : null;
			if (defaultDomain == null) {
				defaultDomain = "";
			}
			else {
				defaultDomain += ".";
			}
			args.add("--spring.jmx.default-domain=" + defaultDomain + "binder." + configurationName);
			args.add("--spring.main.applicationContextClass=" + AnnotationConfigApplicationContext.class.getName());
			List<Class<?>> configurationClasses = new ArrayList<Class<?>>(
					Arrays.asList(binderConfiguration.getBinderType().getConfigurationClasses()));
			SpringApplicationBuilder springApplicationBuilder = new SpringApplicationBuilder()
					.sources(configurationClasses.toArray(new Class<?>[] {})).bannerMode(Mode.OFF).web(false);
			// If the environment is not customized and a main context is available, we
			// will set the latter as parent.
			// This ensures that the defaults and user-defined customizations (e.g. custom
			// connection factory beans)
			// are propagated to the binder context. If the environment is customized,
			// then the binder context should
			// not inherit any beans from the parent
			boolean useApplicationContextAsParent = binderProperties.isEmpty() && this.context != null;
			if (useApplicationContextAsParent) {
				springApplicationBuilder.parent(this.context);
			}
			if (useApplicationContextAsParent || (environment != null && binderConfiguration.isInheritEnvironment())) {
				if (environment != null) {
					StandardEnvironment binderEnvironment = new StandardEnvironment();
					binderEnvironment.merge(environment);
					springApplicationBuilder.environment(binderEnvironment);
				}
			}
			ConfigurableApplicationContext binderProducingContext = springApplicationBuilder
					.run(args.toArray(new String[args.size()]));
			@SuppressWarnings("unchecked")
			Binder<T, ?, ?> binder = binderProducingContext.getBean(Binder.class);
			if (this.listeners != null) {
				for (Listener binderFactoryListener : listeners) {
					binderFactoryListener.afterBinderContextInitialized(configurationName, binderProducingContext);
				}
			}
			this.binderInstanceCache.put(configurationName, new BinderInstanceHolder(binder, binderProducingContext));
		}
		return (Binder<T, ?, ?>) this.binderInstanceCache.get(configurationName).getBinderInstance();
	}

	/**
	 * A listener that can be registered with the {@link DefaultBinderFactory} that allows
	 * the registration of additional configuration.
	 *
	 * @author Ilayaperumal Gopinathan
	 */
	public interface Listener {

		/**
		 * Applying additional capabilities to the binder after the binder context has
		 * been initialized.
		 *
		 * @param configurationName the binder configuration name
		 * @param binderContext the application context of the binder
		 */
		void afterBinderContextInitialized(String configurationName,
				ConfigurableApplicationContext binderContext);
	}

	/**
	 * Utility class for storing {@link Binder} instances, along with their associated
	 * contexts.
	 */
	private static final class BinderInstanceHolder {

		private final Binder<?, ?, ?> binderInstance;

		private final ConfigurableApplicationContext binderContext;

		private BinderInstanceHolder(Binder<?, ?, ?> binderInstance, ConfigurableApplicationContext binderContext) {
			this.binderInstance = binderInstance;
			this.binderContext = binderContext;
		}

		public Binder<?, ?, ?> getBinderInstance() {
			return this.binderInstance;
		}

		public ConfigurableApplicationContext getBinderContext() {
			return this.binderContext;
		}
	}
}
