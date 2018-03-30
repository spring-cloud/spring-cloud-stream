/*
 * Copyright 2015-2018 the original author or authors.
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
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.CompositeMessageChannelConfigurer;
import org.springframework.cloud.stream.binding.ContextStartAfterRefreshListener;
import org.springframework.cloud.stream.binding.DynamicDestinationsBindable;
import org.springframework.cloud.stream.binding.InputBindingLifecycle;
import org.springframework.cloud.stream.binding.MessageChannelConfigurer;
import org.springframework.cloud.stream.binding.MessageChannelStreamListenerResultAdapter;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.binding.MessageSourceBindingTargetFactory;
import org.springframework.cloud.stream.binding.OutputBindingLifecycle;
import org.springframework.cloud.stream.binding.SingleBindingTargetBindable;
import org.springframework.cloud.stream.binding.StreamListenerAnnotationBeanPostProcessor;
import org.springframework.cloud.stream.binding.SubscribableChannelBindingTargetFactory;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.micrometer.DestinationPublishingMetricsAutoConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.config.GlobalChannelInterceptorProcessor;
import org.springframework.integration.config.HandlerMethodArgumentResolversHolder;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.router.AbstractMappingMessageRouter;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.scheduling.TaskScheduler;


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
 */
@Configuration
@EnableConfigurationProperties({ BindingServiceProperties.class, SpringIntegrationProperties.class })
@Import({ContentTypeConfiguration.class, DestinationPublishingMetricsAutoConfiguration.class})
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class BindingServiceConfiguration {

	public static final String STREAM_LISTENER_ANNOTATION_BEAN_POST_PROCESSOR_NAME =
			"streamListenerAnnotationBeanPostProcessor";

	public static final String ERROR_BRIDGE_CHANNEL = "errorBridgeChannel";

	private static final String ERROR_KEY_NAME = "error";


	@Bean
	public MessageChannelStreamListenerResultAdapter messageChannelStreamListenerResultAdapter() {
		return new MessageChannelStreamListenerResultAdapter();
	}

	@Bean
	public static MessageHandlerMethodFactory messageHandlerMethodFactory(CompositeMessageConverterFactory compositeMessageConverterFactory,
			@Qualifier(IntegrationContextUtils.ARGUMENT_RESOLVERS_BEAN_NAME) HandlerMethodArgumentResolversHolder ahmar) {
		DefaultMessageHandlerMethodFactory messageHandlerMethodFactory = new DefaultMessageHandlerMethodFactory();
		messageHandlerMethodFactory.setMessageConverter(compositeMessageConverterFactory.getMessageConverterForAllRegistered());
		messageHandlerMethodFactory.setCustomArgumentResolvers(ahmar.getResolvers());
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
	@ConditionalOnMissingBean
	public BindingService bindingService(BindingServiceProperties bindingServiceProperties,
			BinderFactory binderFactory, TaskScheduler taskScheduler) {
		return new BindingService(bindingServiceProperties, binderFactory, taskScheduler);
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
	public MessageSourceBindingTargetFactory messageSourceFactory(CompositeMessageConverterFactory compositeMessageConverterFactory,
			CompositeMessageChannelConfigurer compositeMessageChannelConfigurer) {
		return new MessageSourceBindingTargetFactory(compositeMessageConverterFactory.getMessageConverterForAllRegistered(), compositeMessageChannelConfigurer);
	}

	@Bean
	@ConditionalOnMissingBean
	public CompositeMessageChannelConfigurer compositeMessageChannelConfigurer(
			MessageConverterConfigurer messageConverterConfigurer) {
		List<MessageChannelConfigurer> configurerList = new ArrayList<>();
		configurerList.add(messageConverterConfigurer);
		return new CompositeMessageChannelConfigurer(configurerList);
	}

	@Bean
	@DependsOn("bindingService")
	public OutputBindingLifecycle outputBindingLifecycle(BindingService bindingService, Map<String, Bindable> bindables) {
		return new OutputBindingLifecycle(bindingService, bindables);
	}

	@Bean
	@DependsOn("bindingService")
	public InputBindingLifecycle inputBindingLifecycle(BindingService bindingService, Map<String, Bindable> bindables) {
		return new InputBindingLifecycle(bindingService, bindables);
	}

	@Bean
	@DependsOn("bindingService")
	public ContextStartAfterRefreshListener contextStartAfterRefreshListener() {
		return new ContextStartAfterRefreshListener();
	}

	@SuppressWarnings("rawtypes")
	@Bean
	public BinderAwareChannelResolver binderAwareChannelResolver(BindingService bindingService,
			AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory,
			DynamicDestinationsBindable dynamicDestinationsBindable,
			@Nullable BinderAwareChannelResolver.NewDestinationBindingCallback callback,
			@Nullable GlobalChannelInterceptorProcessor globalChannelInterceptorProcessor) {
		return new BinderAwareChannelResolver(bindingService, bindingTargetFactory, dynamicDestinationsBindable,
				callback, globalChannelInterceptorProcessor);
	}

	@Bean
	@ConditionalOnProperty("spring.cloud.stream.bindings." + ERROR_KEY_NAME + ".destination")
	public MessageChannel errorBridgeChannel(
			@Qualifier(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME) PublishSubscribeChannel errorChannel) {
		SubscribableChannel errorBridgeChannel = new DirectChannel();
		BridgeHandler handler = new BridgeHandler();
		handler.setOutputChannel(errorBridgeChannel);
		errorChannel.subscribe(handler);
		return errorBridgeChannel;
	}

	@Bean
	@ConditionalOnProperty("spring.cloud.stream.bindings." + ERROR_KEY_NAME + ".destination")
	public SingleBindingTargetBindable<MessageChannel> errorBridgeChannelBindable(
			@Qualifier(ERROR_BRIDGE_CHANNEL) MessageChannel errorBridgeChannel,
			CompositeMessageChannelConfigurer compositeMessageChannelConfigurer) {
		compositeMessageChannelConfigurer.configureOutputChannel(errorBridgeChannel, ERROR_KEY_NAME);
		return new SingleBindingTargetBindable<>(ERROR_KEY_NAME, errorBridgeChannel);
	}

	@Bean
	public DynamicDestinationsBindable dynamicDestinationsBindable() {
		return new DynamicDestinationsBindable();
	}


	@SuppressWarnings("deprecation")
	@Bean
	@ConditionalOnMissingBean
	public org.springframework.cloud.stream.binding.BinderAwareRouterBeanPostProcessor binderAwareRouterBeanPostProcessor(
			@Autowired(required=false) AbstractMappingMessageRouter[] routers,
			@Autowired(required=false)DestinationResolver<MessageChannel> channelResolver) {

		return new org.springframework.cloud.stream.binding.BinderAwareRouterBeanPostProcessor(routers, channelResolver);
	}

	@Bean
	public ApplicationListener<ContextRefreshedEvent> appListener(SpringIntegrationProperties springIntegrationProperties) {
		return new ApplicationListener<ContextRefreshedEvent>() {
			@Override
			public void onApplicationEvent(ContextRefreshedEvent event) {
				event.getApplicationContext().getBeansOfType(AbstractReplyProducingMessageHandler.class).values()
					.forEach(mh -> mh.addNotPropagatedHeaders(springIntegrationProperties.getMessageHandlerNotPropagatedHeaders()));
			}
		};
	}
}
