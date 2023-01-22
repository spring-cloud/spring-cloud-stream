/*
 * Copyright 2017-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.messaging.MessageHandler;

/**
 * A channel for errors. If it has a {@link LastSubscriberMessageHandler} subscriber, it
 * can only have one and it will always be the last subscriber.
 *
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @since 1.3
 *
 */
class BinderErrorChannel extends PublishSubscribeChannel
		implements LastSubscriberAwareChannel {

	private final AtomicInteger subscribers = new AtomicInteger();

	private volatile LastSubscriberMessageHandler finalHandler;

	BinderErrorChannel() {
		super(true);
	}

	@Override
	public boolean subscribe(MessageHandler handler) {
		this.subscribers.incrementAndGet();
		if (handler instanceof LastSubscriberMessageHandler
				&& this.finalHandler != null) {
			throw new IllegalStateException(
					"Only one LastSubscriberMessageHandler is allowed");
		}
		if (this.finalHandler != null) {
			super.unsubscribe(this.finalHandler);
		}
		boolean result = super.subscribe(handler);
		if (this.finalHandler != null) {
			super.subscribe(this.finalHandler);
		}
		if (handler instanceof LastSubscriberMessageHandler lastSubscriberMessageHandler
				&& this.finalHandler == null) {
			this.finalHandler = lastSubscriberMessageHandler;
		}
		return result;
	}

	@Override
	public boolean unsubscribe(MessageHandler handler) {
		this.subscribers.decrementAndGet();
		return super.unsubscribe(handler);
	}

	@Override
	public int subscribers() {
		return this.subscribers.get();
	}

}
