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
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;

/**
 * Class that {@link BindableProxyFactory} uses to create message channels.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 */
public class DefaultChannelFactory implements ChannelFactory {

	@Autowired
	MessageConverterConfigurer messageConverterConfigurer;

	@Override
	public <T extends MessageChannel> T createBindableChannel(String name, Class<T> channelType)
			throws Exception {
		T messageChannel = createMessageChannel(channelType);
		messageConverterConfigurer.configureMessageConverters(messageChannel, name);
		return messageChannel;
	}

	@Override
	public <T extends MessageChannel> T createSharedChannel(Class<T> channelType) throws Exception {
		return createMessageChannel(channelType);
	}

	private <T extends MessageChannel> T createMessageChannel(Class<T> messageChannelType) {
		return isPollable(messageChannelType) ? (T) new QueueChannel() : (T) new DirectChannel();
	}

	private <T extends MessageChannel> boolean isPollable(Class<T> channelType) {
		return PollableChannel.class.equals(channelType);
	}
}
