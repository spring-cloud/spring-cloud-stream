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

package org.springframework.cloud.stream.aggregate;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.springframework.messaging.MessageChannel;

/**
 * A registry for channels that can be shared between modules, used for module aggregation.
 * @author Marius Bogoevici
 */
public class SharedChannelRegistry {

	/**
	 * A {@link Map} of channels, indexed by name. A channel's name may be prefixed by a namespace.
	 */
	private Map<String, MessageChannel> sharedChannels = new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);

	public MessageChannel get(String id) {
		return sharedChannels.get(id);
	}

	public void register(String id, MessageChannel messageChannel) {
		sharedChannels.put(id, messageChannel);
	}

	public Map<String, MessageChannel> getAll() {
		return Collections.unmodifiableMap(sharedChannels);
	}
}
