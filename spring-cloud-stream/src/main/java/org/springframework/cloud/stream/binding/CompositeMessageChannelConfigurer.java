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

import java.util.List;

import org.springframework.messaging.MessageChannel;

/**
 * {@link MessageChannelConfigurer} that composes all the message channel configurers.
 *
 * @author Ilayaperumal Gopinathan
 */
public class CompositeMessageChannelConfigurer implements MessageChannelConfigurer {

	private final List<MessageChannelConfigurer> messageChannelConfigurers;

	public CompositeMessageChannelConfigurer(
			List<MessageChannelConfigurer> messageChannelConfigurers) {
		this.messageChannelConfigurers = messageChannelConfigurers;
	}

	@Override
	public void configureMessageChannel(MessageChannel messageChannel,
			String channelName) {
		for (MessageChannelConfigurer messageChannelConfigurer : this.messageChannelConfigurers) {
			messageChannelConfigurer.configureMessageChannel(messageChannel, channelName);
		}
	}
}
