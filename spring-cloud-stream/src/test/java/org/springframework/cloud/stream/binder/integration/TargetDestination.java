/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.integration;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

/**
 * Implementation of binder endpoint that represents the target destination
 * (e.g., destination which receives messages sent to Processor.OUTPUT)
 * <br>
 * You can interact with it by calling {@link #receive()} operation.
 *
 * @author Oleg Zhurakousky
 *
 */
public class TargetDestination extends AbstractDestination {

	private BlockingQueue<Message<?>> messages;

	/**
	 * Allows to access {@link Message}s received by this {@link TargetDestination}.
	 * @param timeout how long to wait before giving up
	 */
	public Message<?> receive(long timeout) {
		try {
			return this.messages.poll(timeout, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return null;
	}

	/**
	 * Allows to access {@link Message}s received by this {@link TargetDestination}.
	 */
	public Message<?> receive() {
		return this.receive(0);
	}

	@Override
	void afterChannelIsSet() {
		this.messages = new LinkedTransferQueue<>();
		this.getChannel().subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				messages.offer(message);
			}
		});
	}
}
