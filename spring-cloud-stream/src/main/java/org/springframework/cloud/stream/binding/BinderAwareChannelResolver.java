/*
 * Copyright 2013-2016 the original author or authors.
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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
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
 */
public class BinderAwareChannelResolver extends BeanFactoryMessageChannelDestinationResolver {

	private final ChannelBindingService channelBindingService;

	private final BindableChannelFactory bindableChannelFactory;

	private final DynamicDestinationsBindable dynamicDestinationsBindable;

	private ConfigurableListableBeanFactory beanFactory;

	@SuppressWarnings("unchecked")
	public BinderAwareChannelResolver(ChannelBindingService channelBindingService,
			BindableChannelFactory bindableChannelFactory, DynamicDestinationsBindable dynamicDestinationsBindable) {
		this.dynamicDestinationsBindable = dynamicDestinationsBindable;
		Assert.notNull(channelBindingService, "'channelBindingService' cannot be null");
		Assert.notNull(bindableChannelFactory, "'bindableChannelFactory' cannot be null");
		this.channelBindingService = channelBindingService;
		this.bindableChannelFactory = bindableChannelFactory;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		super.setBeanFactory(beanFactory);
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
		}
	}

	@Override
	public MessageChannel resolveDestination(String channelName) {
		MessageChannel channel = null;
		DestinationResolutionException destinationResolutionException;
		try {
			return super.resolveDestination(channelName);
		}
		catch (DestinationResolutionException e) {
			destinationResolutionException = e;
		}
		synchronized (this) {
			if (this.beanFactory != null) {
				String[] dynamicDestinations = null;
				ChannelBindingServiceProperties channelBindingServiceProperties =
						this.channelBindingService.getChannelBindingServiceProperties();
				if (channelBindingServiceProperties != null) {
					dynamicDestinations = channelBindingServiceProperties.getDynamicDestinations();
				}
				boolean dynamicAllowed = ObjectUtils.isEmpty(dynamicDestinations)
						|| ObjectUtils.containsElement(dynamicDestinations, channelName);
				if (dynamicAllowed) {
					channel = this.bindableChannelFactory.createSubscribableChannel(channelName);
					this.beanFactory.registerSingleton(channelName, channel);
					channel = (MessageChannel) this.beanFactory.initializeBean(channel, channelName);
					Binding<MessageChannel> binding = this.channelBindingService.bindProducer(channel, channelName);
					this.dynamicDestinationsBindable.addOutputBinding(channelName, binding);
				}
				else {
					throw destinationResolutionException;
				}
			}
			return channel;
		}
	}
}
