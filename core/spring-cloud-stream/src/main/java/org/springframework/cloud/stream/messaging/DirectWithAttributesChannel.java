/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.cloud.stream.messaging;

import java.util.HashMap;
import java.util.Map;

import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageHandler;

/**
 * @author Oleg Zhurakousky
 * @since 2.1
 */
public class DirectWithAttributesChannel extends DirectChannel {

	/**
	 * Name of the attribute that is considered to be a companion of this channel.
	 */
	public static String COMPANION_ATTR = "companion";

	private final Map<String, Object> attributes = new HashMap<>();

	public void setAttribute(String key, Object value) {
		this.attributes.put(key, value);
	}

	public Object getAttribute(String key) {
		return this.attributes.get(key);
	}

	@Override
	public String getBeanName() {
		return this.getComponentName();
	}

	@Override
	public void destroy() {
		super.destroy();
		Object companion = this.attributes.get(COMPANION_ATTR);
		if (companion != null && companion instanceof AbstractMessageChannel companionChannel) {
			companionChannel.destroy();
		}
	}

	@Override
	public boolean subscribe(MessageHandler handler) {
		return this.getDispatcher().getHandlerCount() == 1 ? false : super.subscribe(handler);
	}
}
