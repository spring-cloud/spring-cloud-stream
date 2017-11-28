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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.reflection.GenericsUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;


/**
 * Default {@link BinderFactory} implementation.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class DefaultBinderFactory implements BinderFactory, DisposableBean, ApplicationContextAware {

	private final Map<String, BinderConfiguration> binderConfigurations;

	private final Map<String, Entry<Binder<?, ?, ?>, ConfigurableApplicationContext>> binderInstanceCache = new HashMap<>();

	private volatile ConfigurableApplicationContext context;

	private final Map<String, String> defaultBinderForBindingTargetType = new HashMap<>();

	private Collection<Listener> listeners;

	private volatile String defaultBinder;

	private final BinderTypeRegistry binderTypeRegistry;

	public DefaultBinderFactory(Map<String, BinderConfiguration> binderConfigurations,
			BinderTypeRegistry binderTypeRegistry) {
		this.binderConfigurations = new HashMap<>(binderConfigurations);
		this.binderTypeRegistry = binderTypeRegistry;
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
		this.binderInstanceCache.values().stream().map(e -> e.getValue()).forEach(ctx -> ctx.close());
		this.defaultBinderForBindingTargetType.clear();
	}

	@Override
	public synchronized <T> Binder<T, ?, ?> getBinder(String name, Class<? extends T> bindingTargetType) {
		String configurationName;
		// Fall back to a default if no argument is provided
		if (StringUtils.isEmpty(name)) {
			Assert.notEmpty(this.binderConfigurations, "A default binder has been requested, but there is no binder available");
			if (!StringUtils.hasText(this.defaultBinder)) {
				Set<String> defaultCandidateConfigurations = new HashSet<>();
				for (Map.Entry<String, BinderConfiguration> binderConfigurationEntry : this.binderConfigurations.entrySet()) {
					if (binderConfigurationEntry.getValue().isDefaultCandidate()) {
						defaultCandidateConfigurations.add(binderConfigurationEntry.getKey());
					}
				}
				if (defaultCandidateConfigurations.size() == 1) {
					configurationName = defaultCandidateConfigurations.iterator().next();
					this.defaultBinderForBindingTargetType.put(bindingTargetType.getName(), configurationName);
				}
				else {
					List<String> candidatesForBindableType = new ArrayList<>();
					for (String defaultCandidateConfiguration : defaultCandidateConfigurations) {
						Binder<Object, ?, ?> binderInstance = getBinderInstance(defaultCandidateConfiguration);
						Class<?> binderType = GenericsUtils.getParameterType(binderInstance.getClass(), Binder.class, 0);
						if (binderType.isAssignableFrom(bindingTargetType)) {
							candidatesForBindableType.add(defaultCandidateConfiguration);
						}
					}
					if (candidatesForBindableType.size() == 1) {
						configurationName = candidatesForBindableType.iterator().next();
						this.defaultBinderForBindingTargetType.put(bindingTargetType.getName(), configurationName);
					}
					else {
						throw new IllegalStateException("A default binder has been requested, but there is more than "
								+ "one binder available for '" + bindingTargetType.getName() + "' : "
								+ StringUtils.collectionToCommaDelimitedString(candidatesForBindableType)
								+ ", and no default binder has been set.");
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
		Assert.state(GenericsUtils.getParameterType(binderInstance.getClass(), Binder.class, 0).isAssignableFrom(bindingTargetType),
				"The binder '" + configurationName + "' cannot bind a " + bindingTargetType.getName());
		return binderInstance;
	}

	@SuppressWarnings("unchecked")
	private <T> Binder<T, ?, ?> getBinderInstance(String configurationName) {
		if (!this.binderInstanceCache.containsKey(configurationName)) {
			BinderConfiguration binderConfiguration = this.binderConfigurations.get(configurationName);
			Assert.state(binderConfiguration != null, "Unknown binder configuration: " + configurationName);
			BinderType binderType = this.binderTypeRegistry.get(binderConfiguration.getBinderType());
			Assert.notNull(binderType, "Binder type " + binderConfiguration.getBinderType() + " is not defined");
			Map<Object, Object> binderProperties = binderConfiguration.getProperties();
			// Convert all properties to arguments, so that they receive maximum
			// precedence
			ArrayList<String> args = new ArrayList<>();
			for (Map.Entry<Object, Object> property : binderProperties.entrySet()) {
				args.add(String.format("--%s=%s", property.getKey(), property.getValue()));
			}
			// Initialize the domain with a unique name based on the bootstrapping context
			// setting
			ConfigurableEnvironment environment = this.context != null ? this.context.getEnvironment() : null;
			String defaultDomain = environment != null ? environment.getProperty("spring.jmx.default-domain.") : "";
			args.add("--spring.jmx.default-domain=" + defaultDomain + "binder." + configurationName);
			args.add("--spring.main.applicationContextClass=" + AnnotationConfigApplicationContext.class.getName());
			SpringApplicationBuilder springApplicationBuilder = new SpringApplicationBuilder()
					.sources(binderType.getConfigurationClasses())
					.bannerMode(Mode.OFF)
					.logStartupInfo(false)
					.web(WebApplicationType.NONE);
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
			if (environment != null && (useApplicationContextAsParent || binderConfiguration.isInheritEnvironment())) {
				StandardEnvironment binderEnvironment = new StandardEnvironment();
				binderEnvironment.merge(environment);
				springApplicationBuilder.environment(binderEnvironment);
			}
			ConfigurableApplicationContext binderProducingContext = springApplicationBuilder
					.run(args.toArray(new String[args.size()]));
			Binder<T, ?, ?> binder = binderProducingContext.getBean(Binder.class);
			if (!CollectionUtils.isEmpty(this.listeners)) {
				for (Listener binderFactoryListener : listeners) {
					binderFactoryListener.afterBinderContextInitialized(configurationName, binderProducingContext);
				}
			}
			this.binderInstanceCache.put(configurationName, new SimpleImmutableEntry<>(binder, binderProducingContext));
		}
		return (Binder<T, ?, ?>) this.binderInstanceCache.get(configurationName).getKey();
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
}
