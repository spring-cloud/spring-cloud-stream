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

package org.springframework.cloud.stream.aggregate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.messaging.MessageChannel;

/**
 * A registry for channels that can be shared between modules. Useful for colocation
 *
 * @author Marius Bogoevici
 */
public class SharedChannelRegistry {

	private Map<String, MessageChannel> sharedChannels = new ConcurrentHashMap<>();

	public SharedChannelRegistry() {
		this(new ConcurrentHashMap<String, MessageChannel>());
	}

	private SharedChannelRegistry(Map<String, MessageChannel> sharedChannels) {
		this.sharedChannels = sharedChannels;
	}

	public MessageChannel getSharedChannel(String id) {
		return sharedChannels.get(id);
	}

	public void putSharedChannel(String id, MessageChannel messageChannel) {
		 sharedChannels.put(id, messageChannel);
	}

	public Map<String, MessageChannel> getSharedChannels() {
		return sharedChannels;
	}
}
