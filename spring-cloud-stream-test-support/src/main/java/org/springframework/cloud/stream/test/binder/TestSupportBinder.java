/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.test.binder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.test.matcher.MessageQueueMatcher;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;

/**
 * A minimal binder that<ul>
 *     <li>does nothing about binding consumers, leaving the channel as-is, so that a test author can interact with it directly,</li>
 *     <li>registers a queue channel on the producer side, so that it is easy to assert what is received.</li>
 * </ul>
 *
 * @author Eric Bottard
 * @see MessageQueueMatcher
 */
public class TestSupportBinder implements Binder<MessageChannel> {

	public static Map<MessageChannel, QueueChannel> channelToBindings =
			new HashMap<>();

	private final MessageCollector messageCollector;

	public TestSupportBinder(MessageCollector messageCollector) {
		this.messageCollector = messageCollector;
	}


	@Override
	public void bindConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {

	}

	/**
	 * Registers a single subscriber to the channel, that enqueues messages for later retrieval and assertion in tests.
	 */
	@Override
	public void bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		final BlockingQueue queue = messageCollector.register(outboundBindTarget);
		((SubscribableChannel)outboundBindTarget).subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				queue.add(message);
			}
		});

	}

	@Override
	public void unbindProducer(String name, MessageChannel channel) {
		messageCollector.unregister(channel);
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outboundBindTarget, Properties properties) {

	}

	@Override
	public void unbindConsumers(String name) {

	}

	@Override
	public void unbindProducers(String name) {

	}

	@Override
	public void unbindConsumer(String name, MessageChannel inboundBindTarget) {

	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {

	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {

	}

	@Override
	public MessageChannel bindDynamicProducer(String name, Properties properties) {
		return null;
	}

	@Override
	public MessageChannel bindDynamicPubSubProducer(String name, Properties properties) {
		return null;
	}

	@Override
	public boolean isCapable(Capability capability) {
		return false;
	}
}
