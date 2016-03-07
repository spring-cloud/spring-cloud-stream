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
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.BeanFactoryMessageChannelDestinationResolver;
import org.springframework.messaging.core.DestinationResolutionException;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * A {@link org.springframework.messaging.core.DestinationResolver} implementation that
 * resolves the channel from the bean factory and, if not present, creates a new channel
 * and adds it to the factory after binding it to the binder. The binder is optionally
 * determined with a prefix preceding a colon.
 * @author Mark Fisher
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 */
public class BinderAwareChannelResolver extends BeanFactoryMessageChannelDestinationResolver {

	private final BinderFactory<MessageChannel> binderFactory;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	private final DynamicDestinationsBindable dynamicDestinationsBindable;

	private ConfigurableListableBeanFactory beanFactory;

	public BinderAwareChannelResolver(BinderFactory binderFactory, ChannelBindingServiceProperties channelBindingServiceProperties,
									  DynamicDestinationsBindable dynamicDestinationsBindable) {
		Assert.notNull(binderFactory, "'binderFactory' cannot be null");
		Assert.notNull(channelBindingServiceProperties, "'channelBindingServiceProperties' cannot be null");
		Assert.notNull(dynamicDestinationsBindable, "'dynamicDestinationBindable' cannot be null");
		this.binderFactory = binderFactory;
		this.channelBindingServiceProperties = channelBindingServiceProperties;
		this.dynamicDestinationsBindable = dynamicDestinationsBindable;
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
			if (this.beanFactory != null && this.binderFactory != null) {
				String[] dynamicDestinations = null;
				if (this.channelBindingServiceProperties != null) {
					dynamicDestinations = this.channelBindingServiceProperties.getDynamicDestinations();
				}
				boolean dynamicAllowed = ObjectUtils.isEmpty(dynamicDestinations)
						|| ObjectUtils.containsElement(dynamicDestinations, channelName);
				if (dynamicAllowed) {
					String binderName = null;
					String beanName = channelName;
					if (channelName.contains(":")) {
						String[] tokens = channelName.split(":", 2);
						if (tokens.length == 2) {
							binderName = tokens[0];
							channelName = tokens[1];
						}
						else if (tokens.length != 1) {
							throw new IllegalArgumentException("Unrecognized channel naming scheme: " + channelName + " , should be" +
									" [<binder>:]<channelName>");
						}
					}
					channel = new DirectChannel();
					this.beanFactory.registerSingleton(beanName, channel);
					channel = (MessageChannel) this.beanFactory.initializeBean(channel, beanName);
					@SuppressWarnings("unchecked")
					Binder<MessageChannel, ?, ProducerProperties> binder =
							(Binder<MessageChannel, ?, ProducerProperties>) binderFactory.getBinder(binderName);
					Class<? extends ProducerProperties> producerPropertiesClass =
							ChannelBindingService.resolveProducerPropertiesType(binder);
					ProducerProperties producerProperties =
							this.channelBindingServiceProperties.getProducerProperties(channelName, producerPropertiesClass);
					String destinationName = this.channelBindingServiceProperties.getBindingDestination(channelName);
					this.dynamicDestinationsBindable.addOutputBinding(beanName,
							binder.bindProducer(destinationName, channel, producerProperties));
				}
				else {
					throw destinationResolutionException;
				}
			}
			return channel;
		}
	}
}
