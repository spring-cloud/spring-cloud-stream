/*
 * Copyright 2015-2016 the original author or authors.
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

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.messaging.MessageChannel;

/**
 * Handles the operations related to channel binding including binding of input/output channels by delegating
 * to an underlying {@link Binder}, setting up data type conversion for binding channel.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
public class ChannelBindingService {

	private BinderFactory<MessageChannel> binderFactory;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	public ChannelBindingService(ChannelBindingServiceProperties channelBindingServiceProperties,
								 BinderFactory<MessageChannel> binderFactory) {
		this.channelBindingServiceProperties = channelBindingServiceProperties;
		this.binderFactory = binderFactory;
	}

	public void bindConsumer(MessageChannel inputChannel, String inputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(inputChannelName);
		Binder<MessageChannel> binder = getBinderForChannel(inputChannelName);
		binder.bindConsumer(channelBindingTarget, consumerGroup(inputChannelName), inputChannel,
				this.channelBindingServiceProperties.getConsumerProperties(inputChannelName));
	}

	public void bindProducer(MessageChannel outputChannel, String outputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(outputChannelName);
		Binder<MessageChannel> binder = getBinderForChannel(outputChannelName);
		binder.bindProducer(channelBindingTarget, outputChannel,
				this.channelBindingServiceProperties.getProducerProperties(outputChannelName));
	}

	public void unbindConsumers(String inputChannelName) {
		Binder<MessageChannel> binder = getBinderForChannel(inputChannelName);
		binder.unbindConsumers(inputChannelName, consumerGroup(inputChannelName));
	}

	public void unbindProducers(String outputChannelName) {
		Binder<MessageChannel> binder = getBinderForChannel(outputChannelName);
		binder.unbindProducers(outputChannelName);
	}

	private Binder<MessageChannel> getBinderForChannel(String channelName) {
		String transport = this.channelBindingServiceProperties.getBinder(channelName);
		return binderFactory.getBinder(transport);
	}

	private String consumerGroup(String inputChannelName) {
		BindingProperties bindingProperties = this.channelBindingServiceProperties.getBindings()
				.get(inputChannelName);
		return bindingProperties == null ? null : bindingProperties.getGroup();
	}

}
