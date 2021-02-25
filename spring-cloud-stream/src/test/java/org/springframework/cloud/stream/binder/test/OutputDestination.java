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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.messaging.Message;
import org.springframework.util.StringUtils;

/**
 * Implementation of binder endpoint that represents the target destination (e.g.,
 * destination which receives messages sent to Processor.OUTPUT) <br>
 * You can interact with it by calling {@link #receive()} operation.
 *
 * @author Oleg Zhurakousky
 *
 */
public class OutputDestination extends AbstractDestination {

	private final Map<String, BlockingQueue<Message<byte[]>>> messageQueues = new LinkedHashMap<>();

	public Message<byte[]> receive(long timeout, String bindingName) {
		try {
			return this.messageQueues.get(bindingName).poll(timeout, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return null;
	}

	/**
	 * Will clear all output destinations.
	 *
	 * @since 3.0.6
	 */
	public void clear() {
		this.messageQueues.values().forEach(v -> v.clear());
	}

	/**
	 * Will clear output destination with specified name.
	 *
	 * @param destinationName the name of the output destination to be cleared.
	 * @return true if attempt to clear specific destination is successful otherwise false.
	 * @since 3.0.6
	 */
	public boolean clear(String destinationName) {
		String queueName = destinationName.endsWith(".destination") ? destinationName : destinationName + ".destination";
		if (StringUtils.hasText(destinationName) && this.messageQueues.containsKey(queueName)) {
			this.messageQueues.get(queueName).clear();
			return true;
		}
		return false;
	}
	/**
	 * Allows to access {@link Message}s received by this {@link OutputDestination}.
	 * @param timeout how long to wait before giving up
	 * @return received message
	 * @deprecated since 3.0.2 in favor of {@link #receive(long, String)} where you should use the actual binding name (e.g., "foo-in-0")
	 */
	@Deprecated
	public Message<byte[]> receive(long timeout, int bindingIndex) {
		try {
			BlockingQueue<Message<byte[]>> destinationQueue = (new ArrayList<>(this.messageQueues.values())).get(bindingIndex);
			return destinationQueue.poll(timeout, TimeUnit.MILLISECONDS);
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
		return this.receive(0, 0);
	}

	public Message<byte[]> receive(long timeout) {
		return this.receive(timeout, 0);
	}

	@SuppressWarnings("unchecked")
	@Override
	void afterChannelIsSet(int channelIndex, String bindingName) {
		if (!this.messageQueues.containsKey(bindingName)) {
			BlockingQueue<Message<byte[]>> messageQueue = new LinkedTransferQueue<>();
			this.messageQueues.put(bindingName, messageQueue);
			if (((AbstractSubscribableChannel) this.getChannelByName(bindingName)).getSubscriberCount() < 1) {
				this.getChannelByName(bindingName).subscribe(message -> this.messageQueues.get(bindingName).offer((Message<byte[]>) message));
			}
		}
	}

}
