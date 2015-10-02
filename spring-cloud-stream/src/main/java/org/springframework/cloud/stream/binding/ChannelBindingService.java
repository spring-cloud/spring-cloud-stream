/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Handles the operations related to channel binding including binding of input/output channels by delegating
 * to an underlying {@link Binder}, setting up data type conversion for binding channel.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class ChannelBindingService {

	@Autowired
	private Binder<MessageChannel> binder;

	@Autowired
	private ChannelBindingServiceProperties channelBindingServiceProperties;

	@Autowired
	private MessageConvertersConfigurer messageConvertersConfigurer;

	public void bindConsumer(MessageChannel inputChannel, String inputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(inputChannelName);
		if (isChannelPubSub(channelBindingTarget)) {
			this.binder.bindPubSubConsumer(removePrefix(channelBindingTarget),
					inputChannel, this.channelBindingServiceProperties.getConsumerProperties(inputChannelName));
		}
		else {
			this.binder.bindConsumer(channelBindingTarget, inputChannel,
					this.channelBindingServiceProperties.getConsumerProperties(inputChannelName));
		}
	}

	public void bindProducer(MessageChannel outputChannel, String outputChannelName) {
		String channelBindingTarget = this.channelBindingServiceProperties.getBindingDestination(outputChannelName);
		if (isChannelPubSub(channelBindingTarget)) {
			this.binder.bindPubSubProducer(removePrefix(channelBindingTarget),
					outputChannel, this.channelBindingServiceProperties.getProducerProperties(outputChannelName));
		}
		else {
			this.binder.bindProducer(channelBindingTarget, outputChannel,
					this.channelBindingServiceProperties.getProducerProperties(outputChannelName));
		}
	}

	private boolean isChannelPubSub(String bindingTarget) {
		Assert.isTrue(StringUtils.hasText(bindingTarget), "Binding target should not be empty/null.");
		return bindingTarget.startsWith("topic:");
	}

	private String removePrefix(String bindingTarget) {
		Assert.isTrue(StringUtils.hasText(bindingTarget), "Binding target should not be empty/null.");
		return bindingTarget.substring(bindingTarget.indexOf(":") + 1);
	}

	public void unbindConsumers(String inputChannelName) {
		this.binder.unbindConsumers(inputChannelName);
	}

	public void unbindProducers(String outputChannelName) {
		this.binder.unbindProducers(outputChannelName);
	}

	/**
	 * Setup data-type and message converters for the given message channel.
	 *
	 * @param channel message channel to set the data-type and message converters
	 * @param channelName the channel name
	 */
	public void configureMessageConverters(Object channel, String channelName) {
		messageConvertersConfigurer.configureMessageConverters(channel, channelName);
	}
}
