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

package org.springframework.cloud.stream.binder.test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.messaging.Message;

/**
 * Implementation of binder endpoint that represents the target destination (e.g.,
 * destination which receives messages sent to Processor.OUTPUT) <br>
 * You can interact with it by calling {@link #receive()} operation.
 *
 * @author Oleg Zhurakousky
 *
 */
public class OutputDestination extends AbstractDestination {

	private BlockingQueue<Message<?>> messages;

	/**
	 * Allows to access {@link Message}s received by this {@link OutputDestination}.
	 * @param timeout how long to wait before giving up
	 * @return received message
	 */
	@SuppressWarnings("unchecked")
	public Message<byte[]> receive(long timeout) {
		try {
			return (Message<byte[]>) this.messages.poll(timeout, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return null;
	}

	/**
	 * Allows to access {@link Message}s received by this {@link OutputDestination}.
	 * @return received message
	 */
	public Message<byte[]> receive() {
		return this.receive(0);
	}

	@Override
	void afterChannelIsSet() {
		this.messages = new LinkedTransferQueue<>();
		this.getChannel().subscribe(message -> this.messages.offer(message));
	}

}
