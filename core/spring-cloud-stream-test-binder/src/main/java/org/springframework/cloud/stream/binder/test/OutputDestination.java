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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

	private final Log log = LogFactory.getLog(OutputDestination.class);

	private final ConcurrentHashMap<String, BlockingQueue<Message<byte[]>>> messageQueues = new ConcurrentHashMap<>();

	public Message<byte[]> receive(long timeout, String bindingName) {
		try {
			bindingName = bindingName.endsWith(".destination") ? bindingName : bindingName + ".destination";
			return this.outputQueue(bindingName).poll(timeout, TimeUnit.MILLISECONDS);
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
	 * This is a convenience method for cases when you only have one binding.
	 * For all other cases use {@link #receive(long, String)} method.
	 *
	 * @return received message
	 */
	public Message<byte[]> receive() {
		return this.receive(0, 0);
	}

	/**
	 * Allows to access {@link Message}s received by this {@link OutputDestination}.
	 * This is a convenience method for cases when you only have one binding.
	 * For all other cases use {@link #receive(long, String)} method.
	 *
	 * @param timeout timeout for receiving message
	 * @return received message
	 */
	public Message<byte[]> receive(long timeout) {
		return this.receive(timeout, 0);
	}

	@SuppressWarnings("unchecked")
	@Override
	void afterChannelIsSet(int channelIndex, String bindingName) {
		if (((AbstractSubscribableChannel) this.getChannelByName(bindingName)).getSubscriberCount() < 1) {
			this.getChannelByName(bindingName).subscribe(message -> this.outputQueue(bindingName).offer((Message<byte[]>) message));
		}
	}

	private BlockingQueue<Message<byte[]>> outputQueue(String bindingName) {
		this.messageQueues.putIfAbsent(bindingName, new LinkedTransferQueue<>());
		return this.messageQueues.get(bindingName);
	}

	private Message<byte[]> receive(long timeout, int bindingIndex) {
		log.warn("!!!While 'receive(long timeout, int bindingIndex)' method may still work it is deprecated no longer supported. "
			+ "It will be removed after 3.1.3 release. Please use 'receive(long timeout, String bindingName)'");
		try {
			BlockingQueue<Message<byte[]>> destinationQueue = (new ArrayList<>(this.messageQueues.values())).get(bindingIndex);
			return destinationQueue.poll(timeout, TimeUnit.MILLISECONDS);
		}
		catch (Exception e) {
			Thread.currentThread().interrupt();
		}
		return null;
	}
}
