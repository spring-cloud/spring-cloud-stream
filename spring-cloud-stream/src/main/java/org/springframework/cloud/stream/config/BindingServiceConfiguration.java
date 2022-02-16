/*
 * Copyright 2015-2021 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.cloud.stream.binder.BinderConfiguration;
import org.springframework.cloud.stream.binder.BinderCustomizer;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.ContextStartAfterRefreshListener;
import org.springframework.cloud.stream.binding.DynamicDestinationsBindable;
import org.springframework.cloud.stream.binding.InputBindingLifecycle;
import org.springframework.cloud.stream.binding.OutputBindingLifecycle;
import org.springframework.cloud.stream.config.BindingHandlerAdvise.MappingsProvider;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.cloud.stream.micrometer.DestinationPublishingMetricsAutoConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Configuration class that provides necessary beans for {@link MessageChannel} binding.
 *
 * @author Dave Syer
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Vinicius Carvalho
 * @author Artem Bilan
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({ BindingServiceProperties.class,
		SpringIntegrationProperties.class, StreamFunctionProperties.class })
@Import({ DestinationPublishingMetricsAutoConfiguration.class,
		SpelExpressionConverterConfiguration.class })
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
@ConditionalOnBean(value = BinderTypeRegistry.class, search = SearchStrategy.CURRENT)
public class BindingServiceConfiguration {

	@Autowired(required = false)
	private Collection<DefaultBinderFactory.Listener> binderFactoryListeners;

	private static Map<String, BinderConfiguration> getBinderConfigurations(
			BinderTypeRegistry binderTypeRegistry,
			BindingServiceProperties bindingServiceProperties) {

		Map<String, BinderConfiguration> binderConfigurations = new HashMap<>();
		Map<String, BinderProperties> declaredBinders = bindingServiceProperties
				.getBinders();
		boolean defaultCandidatesExist = false;
		Iterator<Map.Entry<String, BinderProperties>> binderPropertiesIterator = declaredBinders
				.entrySet().iterator();
		while (!defaultCandidatesExist && binderPropertiesIterator.hasNext()) {
			defaultCandidatesExist = binderPropertiesIterator.next().getValue()
					.isDefaultCandidate();
		}
		List<String> existingBinderConfigurations = new ArrayList<>();
		for (Map.Entry<String, BinderProperties> binderEntry : declaredBinders
				.entrySet()) {
			BinderProperties binderProperties = binderEntry.getValue();
			if (binderTypeRegistry.get(binderEntry.getKey()) != null) {
				binderConfigurations.put(binderEntry.getKey(),
						new BinderConfiguration(binderEntry.getKey(),
								binderProperties.getEnvironment(),
								binderProperties.isInheritEnvironment(),
								binderProperties.isDefaultCandidate()));
				existingBinderConfigurations.add(binderEntry.getKey());
			}
			else {
				Assert.hasText(binderProperties.getType(),
						"No 'type' property present for custom binder "
								+ binderEntry.getKey());
				binderConfigurations.put(binderEntry.getKey(),
						new BinderConfiguration(binderProperties.getType(),
								binderProperties.getEnvironment(),
								binderProperties.isInheritEnvironment(),
								binderProperties.isDefaultCandidate()));
				existingBinderConfigurations.add(binderEntry.getKey());
			}
		}
		for (Map.Entry<String, BinderConfiguration> configurationEntry : binderConfigurations
				.entrySet()) {
			if (configurationEntry.getValue().isDefaultCandidate()) {
				defaultCandidatesExist = true;
			}
		}
		if (!defaultCandidatesExist) {
			for (Map.Entry<String, BinderType> binderEntry : binderTypeRegistry.getAll()
					.entrySet()) {
				if (!existingBinderConfigurations.contains(binderEntry.getKey())) {
					binderConfigurations.put(binderEntry.getKey(),
							new BinderConfiguration(binderEntry.getKey(), new HashMap<>(),
									true, !"integration".equals(binderEntry.getKey())));
				}
			}
		}
		return binderConfigurations;
	}

	@Bean
	public static BeanPostProcessor globalErrorChannelCustomizer() {
		return new BeanPostProcessor() {
			@Override
			public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
				if ("errorChannel".equals(beanName)) {
					((PublishSubscribeChannel) bean).setIgnoreFailures(true);
				}
				return bean;
			}
		};
	}

	@Bean
	public BindingHandlerAdvise BindingHandlerAdvise(
			@Nullable MappingsProvider[] providers) {
		Map<ConfigurationPropertyName, ConfigurationPropertyName> additionalMappings = new HashMap<>();
		if (!ObjectUtils.isEmpty(providers)) {
			for (int i = 0; i < providers.length; i++) {
				MappingsProvider mappingsProvider = providers[i];
				additionalMappings.putAll(mappingsProvider.getDefaultMappings());
			}
		}
		return new BindingHandlerAdvise(additionalMappings);
	}

	@Bean
	@ConditionalOnMissingBean(BinderFactory.class)
	public BinderFactory binderFactory(BinderTypeRegistry binderTypeRegistry,
			BindingServiceProperties bindingServiceProperties,
			ObjectProvider<BinderCustomizer> binderCustomizerProvider) {
		DefaultBinderFactory binderFactory = new DefaultBinderFactory(
				getBinderConfigurations(binderTypeRegistry, bindingServiceProperties),
				binderTypeRegistry, binderCustomizerProvider.getIfUnique());
		binderFactory.setDefaultBinder(bindingServiceProperties.getDefaultBinder());
		binderFactory.setListeners(this.binderFactoryListeners);
		return binderFactory;
	}

	@Bean
	// This conditional is intentionally not in an autoconfig (usually a bad idea) because
	// it is used to detect a BindingService in the parent context (which we know
	// already exists).
	@ConditionalOnMissingBean(search = SearchStrategy.CURRENT)
	public BindingService bindingService(
			BindingServiceProperties bindingServiceProperties,
			BinderFactory binderFactory, TaskScheduler taskScheduler, @Nullable ObjectMapper objectMapper) {
		objectMapper = objectMapper == null ? new ObjectMapper() : objectMapper;
		return new BindingService(bindingServiceProperties, binderFactory, taskScheduler, objectMapper);
	}

	@Bean
	@DependsOn("bindingService")
	public OutputBindingLifecycle outputBindingLifecycle(BindingService bindingService,
			Map<String, Bindable> bindables) {

		return new OutputBindingLifecycle(bindingService, bindables);
	}

	@Bean
	@DependsOn("bindingService")
	public InputBindingLifecycle inputBindingLifecycle(BindingService bindingService,
			Map<String, Bindable> bindables) {
		return new InputBindingLifecycle(bindingService, bindables);
	}

	@Bean
	public BindingsLifecycleController bindingsLifecycleController(List<InputBindingLifecycle> inputBindingLifecycles,
			List<OutputBindingLifecycle> outputBindingsLifecycles) {
		return new BindingsLifecycleController(inputBindingLifecycles, outputBindingsLifecycles);
	}

	@Bean
	@DependsOn("bindingService")
	public ContextStartAfterRefreshListener contextStartAfterRefreshListener() {
		return new ContextStartAfterRefreshListener();
	}

	@Bean
	public DynamicDestinationsBindable dynamicDestinationsBindable() {
		return new DynamicDestinationsBindable();
	}

	@Bean
	public ApplicationListener<ContextRefreshedEvent> appListener(
			SpringIntegrationProperties springIntegrationProperties) {
		return new ApplicationListener<ContextRefreshedEvent>() {

			@Override
			public void onApplicationEvent(ContextRefreshedEvent event) {
				event.getApplicationContext()
						.getBeansOfType(AbstractReplyProducingMessageHandler.class)
						.values()
						.forEach(mh -> mh
								.addNotPropagatedHeaders(springIntegrationProperties
										.getMessageHandlerNotPropagatedHeaders()));
			}

		};
	}

}
