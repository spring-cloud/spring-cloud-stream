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

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.config.ChannelBindingProperties;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Dispatches the binding of input/output channels.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class ChannelBindingAdapter  {

	private Binder<MessageChannel> binder;

	private ChannelBindingProperties channelBindingProperties;

	public ChannelBindingAdapter(ChannelBindingProperties channelBindingProperties,
			Binder<MessageChannel> binder) {
		this.channelBindingProperties = channelBindingProperties;
		this.binder = binder;
	}


	public void bindMessageConsumer(MessageChannel inputChannel, String inputChannelName) {
		String channelBindingTarget = this.channelBindingProperties
				.getBindingPath(inputChannelName);
		if (isChannelPubSub(inputChannelName)) {
			this.binder.bindPubSubConsumer(channelBindingTarget, inputChannel,
					this.channelBindingProperties.getConsumerProperties());
		}
		else {
			this.binder.bindConsumer(channelBindingTarget, inputChannel,
					this.channelBindingProperties.getConsumerProperties());
		}
	}

	public void bindMessageProducer(MessageChannel outputChannel, String outputChannelName) {
		String channelBindingTarget = this.channelBindingProperties
				.getBindingPath(outputChannelName);
		if (isChannelPubSub(outputChannelName)) {
			this.binder.bindPubSubProducer(channelBindingTarget, outputChannel,
					this.channelBindingProperties.getProducerProperties());
		}
		else {
			this.binder.bindProducer(channelBindingTarget, outputChannel,
					this.channelBindingProperties.getProducerProperties());
		}
	}

	private boolean isChannelPubSub(String channelName) {
		Assert.isTrue(StringUtils.hasText(channelName),
				"Channel name should not be empty/null.");
		return channelName.startsWith("topic:");
	}

	public void unbindConsumers(String inputChannelName) {
		this.binder.unbindProducers(inputChannelName);
	}

	public void unbindProducers(String outputChannelName) {
		this.binder.unbindProducers(outputChannelName);
	}


}
