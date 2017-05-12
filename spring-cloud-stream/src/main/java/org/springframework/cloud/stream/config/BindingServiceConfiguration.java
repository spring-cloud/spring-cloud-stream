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

package org.springframework.cloud.stream.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binding.BinderAwareRouterBeanPostProcessor;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.CompositeMessageChannelConfigurer;
import org.springframework.cloud.stream.binding.ContextStartAfterRefreshListener;
import org.springframework.cloud.stream.binding.DynamicDestinationsBindable;
import org.springframework.cloud.stream.binding.InputBindingLifecycle;
import org.springframework.cloud.stream.binding.MessageChannelConfigurer;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.binding.OutputBindingLifecycle;
import org.springframework.cloud.stream.binding.SingleBindingTargetBindable;
import org.springframework.cloud.stream.binding.StreamListenerAnnotationBeanPostProcessor;
import org.springframework.cloud.stream.binding.SubscribableChannelBindingTargetFactory;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.expression.PropertyAccessor;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.config.IntegrationEvaluationContextFactoryBean;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.json.JsonPropertyAccessor;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.core.DestinationResolutionException;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.tuple.spel.TuplePropertyAccessor;
import org.springframework.util.CollectionUtils;

/**
 * Configuration class that provides necessary beans for {@link MessageChannel} binding.
 *
 * @author Dave Syer
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
@Configuration
@EnableConfigurationProperties(BindingServiceProperties.class)
public class BindingServiceConfiguration {

	public static final String STREAM_LISTENER_ANNOTATION_BEAN_POST_PROCESSOR_NAME = "streamListenerAnnotationBeanPostProcessor";

	private static final String ERROR_CHANNEL_NAME = "error";

	@Autowired(required = false)
	private ObjectMapper objectMapper;

	/**
	 * User defined custom message converters
	 */
	@Autowired(required = false)
	private List<MessageConverter> customMessageConverters;

	@Bean
	public static MessageHandlerMethodFactory messageHandlerMethodFactory(
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		DefaultMessageHandlerMethodFactory messageHandlerMethodFactory = new DefaultMessageHandlerMethodFactory();
		messageHandlerMethodFactory
				.setMessageConverter(compositeMessageConverterFactory.getMessageConverterForAllRegistered());
		return messageHandlerMethodFactory;
	}

	@Bean(name = STREAM_LISTENER_ANNOTATION_BEAN_POST_PROCESSOR_NAME)
	public static StreamListenerAnnotationBeanPostProcessor streamListenerAnnotationBeanPostProcessor() {
		return new StreamListenerAnnotationBeanPostProcessor();
	}

	@Bean
	// This conditional is intentionally not in an autoconfig (usually a bad idea) because
	// it is used to detect a BindingService in the parent context (which we know
	// already exists).
	@ConditionalOnMissingBean(BindingService.class)
	public BindingService bindingService(BindingServiceProperties bindingServiceProperties,
			BinderFactory binderFactory) {
		return new BindingService(bindingServiceProperties, binderFactory);
	}

	@Bean
	public MessageConverterConfigurer messageConverterConfigurer(BindingServiceProperties bindingServiceProperties,
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		return new MessageConverterConfigurer(bindingServiceProperties, compositeMessageConverterFactory);
	}

	@Bean
	public SubscribableChannelBindingTargetFactory channelFactory(
			CompositeMessageChannelConfigurer compositeMessageChannelConfigurer) {
		return new SubscribableChannelBindingTargetFactory(compositeMessageChannelConfigurer);
	}

	@Bean
	public CompositeMessageChannelConfigurer compositeMessageChannelConfigurer(
			MessageConverterConfigurer messageConverterConfigurer) {
		List<MessageChannelConfigurer> configurerList = new ArrayList<>();
		configurerList.add(messageConverterConfigurer);
		return new CompositeMessageChannelConfigurer(configurerList);
	}

	@Bean
	@DependsOn("bindingService")
	public OutputBindingLifecycle outputBindingLifecycle() {
		return new OutputBindingLifecycle();
	}

	@Bean
	@DependsOn("bindingService")
	public InputBindingLifecycle inputBindingLifecycle() {
		return new InputBindingLifecycle();
	}

	@Bean
	@DependsOn("bindingService")
	public ContextStartAfterRefreshListener contextStartAfterRefreshListener() {
		return new ContextStartAfterRefreshListener();
	}

	@Bean
	public BinderAwareChannelResolver binderAwareChannelResolver(BindingService bindingService,
			AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory,
			DynamicDestinationsBindable dynamicDestinationsBindable) {
		return new BinderAwareChannelResolver(bindingService, bindingTargetFactory, dynamicDestinationsBindable);
	}

	@Bean
	@ConditionalOnProperty("spring.cloud.stream.bindings." + ERROR_CHANNEL_NAME + ".destination")
	public SingleBindingTargetBindable<MessageChannel> errorChannelBindable(
			@Qualifier(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME) PublishSubscribeChannel errorChannel) {
		return new SingleBindingTargetBindable<MessageChannel>(ERROR_CHANNEL_NAME, errorChannel);
	}

	@Bean
	public DynamicDestinationsBindable dynamicDestinationsBindable() {
		return new DynamicDestinationsBindable();
	}

	@Bean
	public CompositeMessageConverterFactory compositeMessageConverterFactory() {
		List<MessageConverter> messageConverters = new ArrayList<>();
		if (!CollectionUtils.isEmpty(this.customMessageConverters)) {
			messageConverters.addAll(Collections.unmodifiableCollection(this.customMessageConverters));
		}
		return new CompositeMessageConverterFactory(messageConverters, this.objectMapper);
	}

	@Bean
	// provided for backwards compatibility scenarios
	public ChannelBindingServiceProperties channelBindingServiceProperties(
			BindingServiceProperties bindingServiceProperties) {
		return new ChannelBindingServiceProperties(bindingServiceProperties);
	}

	// IMPORTANT: Nested class to avoid instantiating all of the above early
	@Configuration
	protected static class PostProcessorConfiguration {

		private BinderAwareChannelResolver binderAwareChannelResolver;

		/**
		 * Adds property accessors for use in SpEL expression evaluation
		 */
		@Bean
		public static BeanPostProcessor propertyAccessorBeanPostProcessor() {
			final Map<String, PropertyAccessor> accessors = new HashMap<>();
			accessors.put("tuplePropertyAccessor", new TuplePropertyAccessor());
			accessors.put("jsonPropertyAccessor", new JsonPropertyAccessor());
			return new BeanPostProcessor() {

				@Override
				public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
					if (IntegrationContextUtils.INTEGRATION_EVALUATION_CONTEXT_BEAN_NAME.equals(beanName)) {
						IntegrationEvaluationContextFactoryBean factoryBean = (IntegrationEvaluationContextFactoryBean) bean;
						Map<String, PropertyAccessor> factoryBeanAccessors = factoryBean.getPropertyAccessors();
						for (Map.Entry<String, PropertyAccessor> entry : accessors.entrySet()) {
							if (!factoryBeanAccessors.containsKey(entry.getKey())) {
								factoryBeanAccessors.put(entry.getKey(), entry.getValue());
							}
						}
						factoryBean.setPropertyAccessors(factoryBeanAccessors);
					}
					return bean;
				}

				@Override
				public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
					return bean;
				}
			};
		}

		@Bean
		@ConditionalOnMissingBean(BinderAwareRouterBeanPostProcessor.class)
		public BinderAwareRouterBeanPostProcessor binderAwareRouterBeanPostProcessor(
				final ConfigurableListableBeanFactory beanFactory) {
			// IMPORTANT: Lazy delegate to avoid instantiating all of the above early
			return new BinderAwareRouterBeanPostProcessor(new DestinationResolver<MessageChannel>() {

				@Override
				public MessageChannel resolveDestination(String name) throws DestinationResolutionException {
					if (PostProcessorConfiguration.this.binderAwareChannelResolver == null) {
						PostProcessorConfiguration.this.binderAwareChannelResolver = BeanFactoryUtils
								.beanOfType(beanFactory, BinderAwareChannelResolver.class);
					}
					return PostProcessorConfiguration.this.binderAwareChannelResolver.resolveDestination(name);
				}

			});
		}
	}

}
