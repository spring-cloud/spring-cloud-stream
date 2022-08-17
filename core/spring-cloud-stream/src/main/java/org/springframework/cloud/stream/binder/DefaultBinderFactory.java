/*
 * Copyright 2015-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.SpelExpressionConverterConfiguration;
import org.springframework.cloud.stream.reflection.GenericsUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Default {@link BinderFactory} implementation.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @author Artem Bilan
 * @author Anshul Mehra
 * @author Chris Bono
 */
public class DefaultBinderFactory implements BinderFactory, DisposableBean, ApplicationContextAware {

	protected final Log logger = LogFactory.getLog(getClass());

	private final Map<String, BinderConfiguration> binderConfigurations;

	private final Map<String, Entry<Binder<?, ?, ?>, ConfigurableApplicationContext>> binderInstanceCache = new HashMap<>();

	private final Map<String, String> defaultBinderForBindingTargetType = new HashMap<>();

	private final Map<String, ApplicationContextInitializer<ConfigurableApplicationContext>> binderChildContextInitializers = new HashMap<>();

	private final BinderTypeRegistry binderTypeRegistry;

	private final BinderCustomizer binderCustomizer;

	private volatile ConfigurableApplicationContext context;

	private Collection<Listener> listeners;

	private volatile String defaultBinder;

	public DefaultBinderFactory(Map<String, BinderConfiguration> binderConfigurations,
								BinderTypeRegistry binderTypeRegistry, BinderCustomizer binderCustomizer) {
		this.binderConfigurations = new HashMap<>(binderConfigurations);
		this.binderTypeRegistry = binderTypeRegistry;
		this.binderCustomizer = binderCustomizer;
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
	public void destroy() {
		this.binderInstanceCache.values().stream().map(Entry::getValue).forEach(ConfigurableApplicationContext::close);
		this.defaultBinderForBindingTargetType.clear();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public synchronized <T> Binder<T, ?, ?> getBinder(String name,
			Class<? extends T> bindingTargetType) {

		String binderName = StringUtils.hasText(name) ? name : this.defaultBinder;

		Map<String, Binder> binders = this.context == null ? Collections.emptyMap()
				: this.context.getBeansOfType(Binder.class);
		Binder<T, ConsumerProperties, ProducerProperties> binder;
		if (StringUtils.hasText(binderName) && binders.containsKey(binderName)) {
			binder = (Binder<T, ConsumerProperties, ProducerProperties>) this.context
					.getBean(binderName);
		}
		else if (binders.size() == 1) {
			binder = binders.values().iterator().next();
		}
		else if (binders.size() > 1) {
			throw new IllegalStateException(
					"Multiple binders are available, however neither default nor "
							+ "per-destination binder name is provided. Available binders are "
							+ binders.keySet());
		}
		else {
			/*
			 * This is the fall back to the old bootstrap that relies on spring.binders.
			 */
			binder = this.doGetBinder(binderName, bindingTargetType);
		}
		if (this.binderCustomizer != null) {
			this.binderCustomizer.customize(binder, binderName);
		}
		return binder;
	}

	private <T> Binder<T, ConsumerProperties, ProducerProperties> doGetBinder(String name,
			Class<? extends T> bindingTargetType) {

		if (!MessageChannel.class.isAssignableFrom(bindingTargetType)
				&& !PollableMessageSource.class.isAssignableFrom(bindingTargetType)) {
			String bindingTargetTypeName = StringUtils.hasText(name) ? name : bindingTargetType.getSimpleName().toLowerCase();
			Binder<T, ConsumerProperties, ProducerProperties> binderInstance = getBinderInstance(bindingTargetTypeName);
			return binderInstance;
		}
		String configurationName;
		// Fall back to a default if no argument is provided
		if (!StringUtils.hasText(name)) {
			Assert.notEmpty(this.binderConfigurations,
					"A default binder has been requested, but there is no binder available");
			if (!StringUtils.hasText(this.defaultBinder)) {
				Set<String> defaultCandidateConfigurations = new HashSet<>();
				for (Map.Entry<String, BinderConfiguration> binderConfigurationEntry : this.binderConfigurations
						.entrySet()) {
					if (binderConfigurationEntry.getValue().isDefaultCandidate()) {
						defaultCandidateConfigurations
								.add(binderConfigurationEntry.getKey());
					}
				}
				if (defaultCandidateConfigurations.size() == 1) {
					configurationName = defaultCandidateConfigurations.iterator().next();
					this.defaultBinderForBindingTargetType
							.put(bindingTargetType.getName(), configurationName);
				}
				else {
					List<String> candidatesForBindableType = new ArrayList<>();
					for (String defaultCandidateConfiguration : defaultCandidateConfigurations) {
						Binder<Object, ?, ?> binderInstance = getBinderInstance(defaultCandidateConfiguration);

						Class<?> binderType = GenericsUtils.getParameterType(
								binderInstance.getClass(), Binder.class, 0);
						if (binderType.isAssignableFrom(bindingTargetType)) {
							populateCandidatesForBindableType(bindingTargetType, candidatesForBindableType, defaultCandidateConfiguration);
						}
					}
					if (candidatesForBindableType.size() == 1) {
						configurationName = candidatesForBindableType.iterator().next();
						this.defaultBinderForBindingTargetType
								.put(bindingTargetType.getName(), configurationName);
					}
					else {
						String countMsg = (candidatesForBindableType.size() == 0)
								? "are no binders" : "is more than one binder";
						throw new IllegalStateException(
								"A default binder has been requested, but there "
										+ countMsg + " available for '"
										+ bindingTargetType.getName() + "' : "
										+ StringUtils.collectionToCommaDelimitedString(
												candidatesForBindableType)
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
		Binder<T, ConsumerProperties, ProducerProperties> binderInstance = getBinderInstance(configurationName);
		Assert.state(verifyBinderTypeMatchesTarget(binderInstance, bindingTargetType),
				"The binder '" + configurationName + "' cannot bind a "
						+ bindingTargetType.getName());
		return binderInstance;
	}

	private <T> void populateCandidatesForBindableType(Class<? extends T> bindingTargetType, List<String> candidatesForBindableType,
													String defaultCandidateConfiguration) {
		// Going by the convention of proper reactor based binders start with the key literal - reactor
		boolean isCandidate = (FluxMessageChannel.class.isAssignableFrom(bindingTargetType) && defaultCandidateConfiguration.startsWith("reactor"))
				|| !defaultCandidateConfiguration.startsWith("reactor");
		if (isCandidate) {
			candidatesForBindableType.add(defaultCandidateConfiguration);
		}
	}

	/**
	 * Return true if the binder is a {@link PollableConsumerBinder} and the target type
	 * is a {@link PollableSource} and their generic types match (e.g. MessageHandler), OR
	 * if it's a {@link Binder} and the target matches the binder's generic type.
	 * @param <T> bindng target type
	 * @param binderInstance the binder.
	 * @param bindingTargetType the binding target type.
	 * @return true if the conditions match.
	 */
	private <T> boolean verifyBinderTypeMatchesTarget(Binder<T, ?, ?> binderInstance,
			Class<? extends T> bindingTargetType) {
		return (binderInstance instanceof PollableConsumerBinder && GenericsUtils
				.checkCompatiblePollableBinder(binderInstance, bindingTargetType))
				|| GenericsUtils
						.getParameterType(binderInstance.getClass(), Binder.class, 0)
						.isAssignableFrom(bindingTargetType);
	}

	@SuppressWarnings("unchecked")
	private <T> Binder<T, ConsumerProperties, ProducerProperties> getBinderInstance(String configurationName) {
		if (!this.binderInstanceCache.containsKey(configurationName)) {
			this.logger.info("Creating binder: " + configurationName);
			BinderConfiguration binderConfiguration = this.binderConfigurations.get(configurationName);
			Assert.state(binderConfiguration != null, "Unknown binder configuration: " + configurationName);
			BinderType binderType = this.binderTypeRegistry.get(binderConfiguration.getBinderType());
			Assert.notNull(binderType, "Binder type " + binderConfiguration.getBinderType() + " is not defined");
			Map<String, Object> binderProperties = new HashMap<>();
			this.flatten(null, binderConfiguration.getProperties(), binderProperties);

			ConfigurableApplicationContext binderProducingContext;
			if (this.binderChildContextInitializers.containsKey(configurationName)) {
				this.logger.info("Using AOT pre-prepared initializer to construct binder child context for " + configurationName);
				binderProducingContext = this.createUnitializedContextForAOT(configurationName, binderProperties, binderConfiguration);
				this.binderChildContextInitializers.get(configurationName).initialize(binderProducingContext);
				binderProducingContext.refresh();
			}
			else {
				this.logger.info("Constructing binder child context for " + configurationName);
				binderProducingContext = this.initializeBinderContextSimple(configurationName, binderProperties,
						binderType, binderConfiguration, true);
			}

			Map<String, MessageConverter> messageConverters = binderProducingContext.getBeansOfType(MessageConverter.class);
			if (!CollectionUtils.isEmpty(messageConverters) && !ObjectUtils.isEmpty(context.getBeansOfType(FunctionCatalog.class))) {
				FunctionCatalog functionCatalog = this.context.getBean(FunctionCatalog.class);
				if (functionCatalog instanceof SimpleFunctionRegistry) {
					((SimpleFunctionRegistry) functionCatalog).addMessageConverters(messageConverters.values());
				}
			}

			Binder<T, ?, ?> binder = binderProducingContext.getBean(Binder.class);
			/*
			 * This will ensure that application defined errorChannel and other beans are
			 * accessible within binder's context (see
			 * https://github.com/spring-cloud/spring-cloud-stream/issues/1384)
			 */
			if (this.context != null && binder instanceof ApplicationContextAware) {
				((ApplicationContextAware) binder).setApplicationContext(this.context);
			}
			if (!CollectionUtils.isEmpty(this.listeners)) {
				for (Listener binderFactoryListener : this.listeners) {
					binderFactoryListener.afterBinderContextInitialized(configurationName,
							binderProducingContext);
				}
			}
			logger.info("Caching the binder: " + configurationName);
			this.binderInstanceCache.put(configurationName,
					new SimpleImmutableEntry<>(binder, binderProducingContext));
		}
		logger.trace("Retrieving cached binder: " + configurationName);
		return (Binder<T, ConsumerProperties, ProducerProperties>) this.binderInstanceCache
				.get(configurationName).getKey();
	}


	/**
	 * @return map of binder name to binder configuration
	 */
	Map<String, BinderConfiguration> getBinderConfigurations() {
		return this.binderConfigurations;
	}

	/**
	 * Creates a binder child application context that can be used by AOT for pre-generation.
	 * @param configurationName binder configuration name
	 * @return binder child application context that has not been refreshed
	 */
	@SuppressWarnings("rawtypes")
	ConfigurableApplicationContext createBinderContextForAOT(String configurationName) {
		logger.info("Pre-creating binder child context (AOT) for " + configurationName);
		BinderConfiguration binderConfiguration = this.binderConfigurations.get(configurationName);
		Assert.state(binderConfiguration != null, "Unknown binder configuration: " + configurationName);
		BinderType binderType = this.binderTypeRegistry.get(binderConfiguration.getBinderType());
		Assert.notNull(binderType, "Binder type " + binderConfiguration.getBinderType() + " is not defined");
		Map<String, Object> binderProperties = new HashMap<>();
		this.flatten(null, binderConfiguration.getProperties(), binderProperties);
		return initializeBinderContextSimple(configurationName, binderProperties, binderType, binderConfiguration, false);
	}

	/**
	 * Sets the initializers to use when populating a binder child application context.
	 * <p>
	 * This is useful for the AOT scenario where the child binder contexts have been pre-generated into the form of an
	 * application context initializer.
	 *
	 * @param binderChildContextInitializers map of binder configuration name to initializer for the binder child context
	 */
	void setBinderChildContextInitializers(Map<String, ApplicationContextInitializer<ConfigurableApplicationContext>> binderChildContextInitializers) {
		this.binderChildContextInitializers.clear();
		this.binderChildContextInitializers.putAll(binderChildContextInitializers);
	}

	/**
	 * Creates and optionally refreshes a binder child application context.
	 * @param configurationName binder configuration name
	 * @param binderProperties binder properties
	 * @param binderType binder type
	 * @param binderConfiguration binder configuration
	 * @param refresh whether to refresh the context
	 * @return refreshed binder child application context
	 */
	@SuppressWarnings("rawtypes")
	ConfigurableApplicationContext initializeBinderContextSimple(String configurationName, Map<String, Object> binderProperties,
			BinderType binderType, BinderConfiguration binderConfiguration, boolean refresh) {
		AnnotationConfigApplicationContext binderProducingContext = new AnnotationConfigApplicationContext();

		List<Class> sourceClasses = new ArrayList<>();
		sourceClasses.addAll(Arrays.asList(binderType.getConfigurationClasses()));
		sourceClasses.addAll(Collections.singletonList(SpelExpressionConverterConfiguration.class));
		if (binderProperties.containsKey("spring.main.sources")) {
			String sources = (String) binderProperties.get("spring.main.sources");
			if (StringUtils.hasText(sources)) {
				Stream.of(sources.split(",")).forEach(source -> {
					try {
						sourceClasses.add(Thread.currentThread().getContextClassLoader().loadClass(source.trim()));
					}
					catch (Exception e) {
						throw new IllegalStateException("Failed to load class " + source, e);
					}
				});
			}
		}

		binderProducingContext.register(sourceClasses.toArray(new Class[] {}));
		MapPropertySource binderPropertySource = new MapPropertySource(configurationName, binderProperties);
		binderProducingContext.getEnvironment().getPropertySources().addFirst(binderPropertySource);
		binderProducingContext.setDisplayName(configurationName + "_context");
		boolean useApplicationContextAsParent = binderProperties.isEmpty()
				&& this.context != null;
		ConfigurableEnvironment environment = this.context != null
				? this.context.getEnvironment() : null;
		if (useApplicationContextAsParent) {
			binderProducingContext.setParent(this.context);
		}
		else if (this.context != null) {
			Map<String, ListenerContainerCustomizer> customizers = this.context.getBeansOfType(ListenerContainerCustomizer.class);
			if (!CollectionUtils.isEmpty(customizers)) {
				for (Entry<String, ListenerContainerCustomizer> customizerEntry : customizers.entrySet()) {
					ListenerContainerCustomizer customizerWrapper = new ListenerContainerCustomizer() {
						@SuppressWarnings("unchecked")
						@Override
						public void configure(Object container, String destinationName, String group) {
							try {
								customizerEntry.getValue().configure(container, destinationName, group);
							}
							catch (Exception e) {
								logger.warn("Failed while applying ListenerContainerCustomizer. In situations when multiple "
										+ "binders are used this is expected, since a particular customizer may not be applicable.");
							}
						}
					};

					((GenericApplicationContext) binderProducingContext).registerBean(customizerEntry.getKey(),
							ListenerContainerCustomizer.class, () -> customizerWrapper);
				}
			}
			binderProducingContext.addApplicationListener(new ApplicationListener<ApplicationEvent>() {
				@Override
				public void onApplicationEvent(ApplicationEvent event) {
					if (context != null) {
						try {
							context.publishEvent(event);
						}
						catch (Exception e) {
							logger.warn("Failed to publish " + event, e);
						}
					}
				}
			});

			if (environment != null && !useApplicationContextAsParent) {
				InitializerWithOuterContext initializer = new InitializerWithOuterContext(this.context);
				initializer.initialize(binderProducingContext);
			}

			if (environment != null && (useApplicationContextAsParent
					|| binderConfiguration.isInheritEnvironment())) {
				binderProducingContext.getEnvironment().merge(environment);
				binderProducingContext.getEnvironment().getPropertySources().remove("configurationProperties");
				binderProducingContext.getEnvironment().getPropertySources()
				.addFirst(new MapPropertySource("defaultBinderFactoryProperties",
					Collections.singletonMap("spring.main.web-application-type", "NONE")));
			}
		}

		if (refresh) {
			binderProducingContext.refresh();
		}

		return binderProducingContext;
	}

	/**
	 * Creates a bare minimum application context that can be initialized by AOT.
	 *
	 * @param configurationName binder configuration name
	 * @param binderProperties binder properties
	 * @param binderConfiguration binder configuration
	 * @return a binder child application context suitable for AOT initialization
	 */
	GenericApplicationContext createUnitializedContextForAOT(String configurationName,
			Map<String, Object> binderProperties, BinderConfiguration binderConfiguration) {

		GenericApplicationContext binderContext = new GenericApplicationContext();

		MapPropertySource binderPropertySource = new MapPropertySource(configurationName, binderProperties);
		binderContext.getEnvironment().getPropertySources().addFirst(binderPropertySource);
		binderContext.setDisplayName(configurationName + "_context");
		boolean useApplicationContextAsParent = binderProperties.isEmpty() && this.context != null;
		ConfigurableEnvironment environment = this.context != null ? this.context.getEnvironment() : null;
		if (useApplicationContextAsParent) {
			binderContext.setParent(this.context);
		}
		else if (this.context != null) {
			binderContext.addApplicationListener(event -> {
				if (context != null) {
					try {
						context.publishEvent(event);
					}
					catch (Exception e) {
						logger.warn("Failed to publish " + event, e);
					}
				}
			});
			if (environment != null && (useApplicationContextAsParent || binderConfiguration.isInheritEnvironment())) {
				binderContext.getEnvironment().merge(environment);
				binderContext.getEnvironment().getPropertySources().remove("configurationProperties");
				binderContext.getEnvironment().getPropertySources()
						.addFirst(new MapPropertySource("defaultBinderFactoryProperties",
								Collections.singletonMap("spring.main.web-application-type", "NONE")));
			}
		}
		return binderContext;
	}

	/**
	 * Ensures that nested properties are flattened (i.e., foo.bar=baz instead of
	 * foo={bar=baz}).
	 * @param propertyName property name to flatten
	 * @param value value that contains the property name
	 * @param flattenedProperties map to which we'll add the falttened property
	 */
	@SuppressWarnings("unchecked")
	private void flatten(String propertyName, Object value, Map<String, Object> flattenedProperties) {
		if (value instanceof Map) {
			((Map<Object, Object>) value).forEach((k, v) -> flatten(
					(propertyName != null ? propertyName + "." : "") + k, v, flattenedProperties));
		}
		else {
			flattenedProperties.put(propertyName, value.toString());
		}
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
		 * @param configurationName the binder configuration name
		 * @param binderContext the application context of the binder
		 */
		void afterBinderContextInitialized(String configurationName,
				ConfigurableApplicationContext binderContext);

	}

	private static class InitializerWithOuterContext
			implements ApplicationContextInitializer<ConfigurableApplicationContext> {

		private final ApplicationContext context;

		InitializerWithOuterContext(ApplicationContext context) {
			this.context = context;
		}

		@Override
		public void initialize(ConfigurableApplicationContext applicationContext) {
			applicationContext.getBeanFactory().registerSingleton("outerContext",
					this.context);
		}

	}
}
