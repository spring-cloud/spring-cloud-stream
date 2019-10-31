/*
 * Copyright 2013-2019 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
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
 *
 * @deprecated As of 3.0.0 in favor if providing `spring.cloud.stream.sendto.destination` property.
 * This is primarily for function-based programming model. For StreamListener it would still be
 * required and thus will stay until we deprecate and eventually discontinue StreamListener
 * and annotation-based programming model.
 */
@Deprecated
public class BinderAwareChannelResolver
		extends BeanFactoryMessageChannelDestinationResolver {

	private final BindingService bindingService;

	private final AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory;

	private final DynamicDestinationsBindable dynamicDestinationsBindable;

	@SuppressWarnings("rawtypes")
	private final NewDestinationBindingCallback newBindingCallback;

	private ConfigurableListableBeanFactory beanFactory;

	public BinderAwareChannelResolver(BindingService bindingService,
			AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory,
			DynamicDestinationsBindable dynamicDestinationsBindable) {
		this(bindingService, bindingTargetFactory, dynamicDestinationsBindable, null);
	}

	@SuppressWarnings("rawtypes")
	public BinderAwareChannelResolver(BindingService bindingService,
			AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory,
			DynamicDestinationsBindable dynamicDestinationsBindable,
			NewDestinationBindingCallback callback) {
		this.dynamicDestinationsBindable = dynamicDestinationsBindable;
		Assert.notNull(bindingService, "'bindingService' cannot be null");
		Assert.notNull(bindingTargetFactory, "'bindingTargetFactory' cannot be null");
		this.bindingService = bindingService;
		this.bindingTargetFactory = bindingTargetFactory;
		this.newBindingCallback = callback;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		super.setBeanFactory(beanFactory);
		Assert.isTrue(beanFactory instanceof ConfigurableListableBeanFactory,
				"'beanFactory' must be an instance of ConfigurableListableBeanFactory");
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	/*
	 * See the following for more discussion on it as well as demo reproducing it, thanks
	 * to Anshul Mehra (@Walliee)
	 * https://github.com/spring-cloud/spring-cloud-stream/issues/1603
	 */
	@SuppressWarnings("unchecked")
	@Override
	public synchronized MessageChannel resolveDestination(String channelName) {
		BindingServiceProperties bindingServiceProperties = this.bindingService
				.getBindingServiceProperties();
		String[] dynamicDestinations = bindingServiceProperties.getDynamicDestinations();

		MessageChannel channel;
		boolean dynamicAllowed = ObjectUtils.isEmpty(dynamicDestinations)
				|| ObjectUtils.containsElement(dynamicDestinations, channelName);
		try {
			channel = super.resolveDestination(channelName);
		}
		catch (DestinationResolutionException e) {
			if (!dynamicAllowed) {
				throw e;
			}
			else {
				channel = this.bindingTargetFactory.createOutput(channelName);
				ProducerProperties producerProperties = bindingServiceProperties
						.getProducerProperties(channelName);
				if (this.newBindingCallback != null) {
					Object extendedProducerProperties = this.bindingService
							.getExtendedProducerProperties(channel, channelName);
					this.newBindingCallback.configure(channelName, channel,
							producerProperties, extendedProducerProperties);
				}
				bindingServiceProperties.updateProducerProperties(channelName,
						producerProperties);
				this.beanFactory.registerSingleton(channelName, channel);
				channel = (MessageChannel) this.beanFactory.initializeBean(channel,
						channelName);
				Binding<MessageChannel> binding = this.bindingService
						.bindProducer(channel, channelName);
				this.dynamicDestinationsBindable.addOutputBinding(channelName, binding);
			}
		}
		return channel;
	}

	/**
	 * Configure a new destination before it is bound.
	 *
	 * @param <T> the extended properties type. If you need to support dynamic binding
	 * with multiple binders, use {@link Object} and cast as needed.
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
		 * depends on binder type and may be null if the binder doesn't support extended
		 * properties).
		 */
		void configure(String channelName, MessageChannel channel,
				ProducerProperties producerProperties, T extendedProducerProperties);

	}

}
