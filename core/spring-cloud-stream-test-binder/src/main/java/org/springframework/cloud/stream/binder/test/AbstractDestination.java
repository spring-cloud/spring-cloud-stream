/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.cloud.stream.binder.test;

import java.util.ArrayList;
import java.util.List;

import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * @author Oleg Zhurakousky
 *
 */
abstract class AbstractDestination {

	private final List<AbstractSubscribableChannel> channels = new ArrayList<>();

	SubscribableChannel getChannel(int index) {
		return this.channels.get(index);
	}

	void setChannel(SubscribableChannel channel) {
		this.channels.add((AbstractSubscribableChannel) channel);
		this.afterChannelIsSet(this.channels.size() - 1, ((AbstractSubscribableChannel) channel).getBeanName());
	}

	void afterChannelIsSet(int channelIndex, String name) {
		// noop
	}

	SubscribableChannel getChannelByName(String name) {
		name = name.endsWith(".destination") ? name : name + ".destination";
		for (AbstractSubscribableChannel subscribableChannel : channels) {
			if (subscribableChannel.getBeanName().equals(name)) {
				return subscribableChannel;
			}
		}
		return null;
	}
}
