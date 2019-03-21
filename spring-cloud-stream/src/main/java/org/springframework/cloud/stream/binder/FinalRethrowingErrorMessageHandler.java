/*
 * Copyright 2017-2019 the original author or authors.
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

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

/**
 * A MessageHandler that always is the last subscriber (on a {@link BinderErrorChannel})
 * that throws an exception if it the only subscriber (aside from the bridge to the global
 * error channel). It is typically only used if a binder implementation does not return a
 * handled from {@code getErrorMessageHandler()}.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
class FinalRethrowingErrorMessageHandler
		implements MessageHandler, LastSubscriberMessageHandler {

	private final LastSubscriberAwareChannel errorChannel;

	private final boolean defaultErrorChannelPresent;

	FinalRethrowingErrorMessageHandler(LastSubscriberAwareChannel errorChannel,
			boolean defaultErrorChannelPresent) {
		this.errorChannel = errorChannel;
		this.defaultErrorChannelPresent = defaultErrorChannelPresent;
	}

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		if (this.errorChannel.subscribers() > (this.defaultErrorChannelPresent ? 2 : 1)) {
			// user has subscribed; default is 2, this and the bridge to the
			// errorChannel
			return;
		}
		if (message.getPayload() instanceof MessagingException) {
			throw (MessagingException) message.getPayload();
		}
		else {
			throw new MessagingException((Message<?>) null,
					(Throwable) message.getPayload());
		}
	}

}
