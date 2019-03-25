/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.aggregate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.messaging.MessageChannel;

/**
 * Wraps the {@link SharedBindingTargetRegistry} for access to {@link MessageChannel}
 * instances. This class is provided as a convenience for users of
 * {@link SharedChannelRegistry} in previous versions and will be removed in the future.
 *
 * @author Marius Bogoevici
 * @deprecated in favor of {@link SharedBindingTargetRegistry}. Will be removed in 3.0.
 * Not currently used by the framework.
 */
@Deprecated
public class SharedChannelRegistry {

	private final SharedBindingTargetRegistry sharedBindingTargetRegistry;

	public SharedChannelRegistry(
			SharedBindingTargetRegistry sharedBindingTargetRegistry) {
		this.sharedBindingTargetRegistry = sharedBindingTargetRegistry;
	}

	public MessageChannel get(String id) {
		return this.sharedBindingTargetRegistry.get(id, MessageChannel.class);
	}

	public void register(String id, MessageChannel bindingTarget) {
		this.sharedBindingTargetRegistry.register(id, bindingTarget);
	}

	public Map<String, MessageChannel> getAll() {
		Map<String, Object> sharedBindingTargets = this.sharedBindingTargetRegistry
				.getAll();
		Map<String, MessageChannel> sharedMessageChannels = new HashMap<>();
		for (Map.Entry<String, Object> sharedBindingTargetEntry : sharedBindingTargets
				.entrySet()) {
			if (MessageChannel.class
					.isAssignableFrom(sharedBindingTargetEntry.getValue().getClass())) {
				sharedMessageChannels.put(sharedBindingTargetEntry.getKey(),
						(MessageChannel) sharedBindingTargetEntry.getValue());
			}
		}
		return Collections.unmodifiableMap(sharedMessageChannels);
	}

}
