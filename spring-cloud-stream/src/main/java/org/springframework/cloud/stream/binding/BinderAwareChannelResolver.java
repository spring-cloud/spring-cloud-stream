/*
 * Copyright 2013-2018 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.integration.channel.ChannelInterceptorAware;
import org.springframework.integration.config.GlobalChannelInterceptorProcessor;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.BeanFactoryMessageChannelDestinationResolver;
import org.springframework.messaging.core.DestinationResolutionException;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * A {@link org.springframework.messaging.core.DestinationResolver} implementation that
 * resolves the channel from the bean factory and, if not present, creates a new channel
 * and adds it to the factory after binding it to the binder.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
public class BinderAwareChannelResolver extends BeanFactoryMessageChannelDestinationResolver {

	private final Log logger = LogFactory.getLog(BinderAwareChannelResolver.class);

	private final BindingService bindingService;

	private final AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory;

	private final DynamicDestinationsBindable dynamicDestinationsBindable;

	@SuppressWarnings("rawtypes")
	private final NewDestinationBindingCallback newBindingCallback;

	private ConfigurableListableBeanFactory beanFactory;

	private final GlobalChannelInterceptorProcessor globalChannelInterceptorProcessor;

	public BinderAwareChannelResolver(BindingService bindingService,
			AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory,
			DynamicDestinationsBindable dynamicDestinationsBindable) {
		this(bindingService, bindingTargetFactory, dynamicDestinationsBindable, null, null);
	}

	@SuppressWarnings("rawtypes")
	public BinderAwareChannelResolver(BindingService bindingService,
			AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory,
			DynamicDestinationsBindable dynamicDestinationsBindable, NewDestinationBindingCallback callback) {
		this(bindingService, bindingTargetFactory, dynamicDestinationsBindable, callback, null);
	}

	@SuppressWarnings("rawtypes")
	public BinderAwareChannelResolver(BindingService bindingService,
			AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory,
			DynamicDestinationsBindable dynamicDestinationsBindable, NewDestinationBindingCallback callback,
			GlobalChannelInterceptorProcessor globalChannelInterceptorProcessor) {
		this.dynamicDestinationsBindable = dynamicDestinationsBindable;
		Assert.notNull(bindingService, "'bindingService' cannot be null");
		Assert.notNull(bindingTargetFactory, "'bindingTargetFactory' cannot be null");
		this.bindingService = bindingService;
		this.bindingTargetFactory = bindingTargetFactory;
		this.newBindingCallback = callback;
		this.globalChannelInterceptorProcessor = globalChannelInterceptorProcessor;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		super.setBeanFactory(beanFactory);
		Assert.isTrue(beanFactory instanceof ConfigurableListableBeanFactory, "'beanFactory' must be an instance of ConfigurableListableBeanFactory");
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	@SuppressWarnings("unchecked")
	@Override
	public MessageChannel resolveDestination(String channelName) {
		try {
			return super.resolveDestination(channelName);
		}
		catch (DestinationResolutionException e) {
			// intentionally empty; will check again while holding the monitor
		}
		synchronized (this) {
			BindingServiceProperties bindingServiceProperties = this.bindingService.getBindingServiceProperties();
			String[] dynamicDestinations = bindingServiceProperties.getDynamicDestinations();
			boolean dynamicAllowed = ObjectUtils.isEmpty(dynamicDestinations) || ObjectUtils.containsElement(dynamicDestinations, channelName);
			try {
				return super.resolveDestination(channelName);
			}
			catch (DestinationResolutionException e) {
				if (!dynamicAllowed) {
					throw e;
				}
			}

			MessageChannel channel = this.bindingTargetFactory.createOutput(channelName);
			this.beanFactory.registerSingleton(channelName, channel);

			//TODO: Investigate if the following call is necessary.
			//initializeBean call on the next line also calling the addMatchingInterceptors method in GlobalChannelInterceptorProcessor
			//this.instrumentChannelWithGlobalInterceptors(channel, channelName);

			channel = (MessageChannel) this.beanFactory.initializeBean(channel, channelName);
			if (this.newBindingCallback != null) {
				ProducerProperties producerProperties = bindingServiceProperties.getProducerProperties(channelName);
				Object extendedProducerProperties = this.bindingService.getExtendedProducerProperties(channel, channelName);
				this.newBindingCallback.configure(channelName, channel, producerProperties, extendedProducerProperties);
				bindingServiceProperties.updateProducerProperties(channelName, producerProperties);
			}
			Binding<MessageChannel> binding = this.bindingService.bindProducer(channel, channelName);
			this.dynamicDestinationsBindable.addOutputBinding(channelName, binding);

			return channel;
		}
	}

	private void instrumentChannelWithGlobalInterceptors(MessageChannel channel, String channelName) {
		if (channel instanceof ChannelInterceptorAware) {
			if (this.globalChannelInterceptorProcessor != null) {
				this.globalChannelInterceptorProcessor.addMatchingInterceptors((ChannelInterceptorAware) channel, channelName);
			}
		}
		else {
			logger.warn("Failed to add global interceptors to '" + channelName + "' since it is not an instance of ChannelInterceptorAware.");
		}
	}

	/**
	 * Configure a new destination before it is bound.
	 * @param <T> the extended properties type. If you need to support dynamic binding
	 * with multiple binders, use {@link Object} and cast as needed.
	 *
	 * @since 2.0
	 *
	 */
	@FunctionalInterface
	public interface NewDestinationBindingCallback<T> {

		/**
		 * Configure the properties or channel before binding.
		 * @param channelName the name of the new channel.
		 * @param channel the channel that is about to be bound.
		 * @param producerProperties the producer properties.
		 * @param extendedProducerProperties the extended producer properties (type
		 * depends on binder type and may be null if the binder doesn't support
		 * extended properties).
		 */
		void configure(String channelName, MessageChannel channel, ProducerProperties producerProperties,
				T extendedProducerProperties);

	}
}
