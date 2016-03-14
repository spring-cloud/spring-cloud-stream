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

import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * Class that {@link BindableProxyFactory} uses to create and configure message channels.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 */
public class DefaultBindableChannelFactory implements BindableChannelFactory {

	private final MessageChannelConfigurer messageChannelConfigurer;

	public DefaultBindableChannelFactory(MessageChannelConfigurer messageChannelConfigurer) {
		this.messageChannelConfigurer = messageChannelConfigurer;
	}

	@Override
	public PollableChannel createPollableChannel(String name) {
		PollableChannel pollableChannel = new QueueChannel();
		messageChannelConfigurer.configureMessageChannel(pollableChannel, name);
		return pollableChannel;
	}

	@Override
	public SubscribableChannel createSubscribableChannel(String name) {
		SubscribableChannel subscribableChannel = new DirectChannel();
		messageChannelConfigurer.configureMessageChannel(subscribableChannel, name);
		return subscribableChannel;
	}


}
