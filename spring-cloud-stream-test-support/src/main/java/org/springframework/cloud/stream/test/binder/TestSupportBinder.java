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

package org.springframework.cloud.stream.test.binder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.test.matcher.MessageQueueMatcher;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;

/**
 * A minimal binder that<ul>
 *     <li>does nothing about binding consumers, leaving the channel as-is, so that a test author can interact with it directly,</li>
 *     <li>registers a queue channel on the producer side, so that it is easy to assert what is received.</li>
 * </ul>
 *
 * @author Eric Bottard
 * @author Gary Russell
 * @author Mark Fisher
 * @see MessageQueueMatcher
 */
public class TestSupportBinder implements Binder<MessageChannel> {

	private final MessageCollectorImpl messageCollector = new MessageCollectorImpl();

	private final ConcurrentMap<String, MessageChannel> messageChannels = new ConcurrentHashMap<>();

	@Override
	public Binding<MessageChannel> bindConsumer(String name, String group, MessageChannel inboundBindTarget, Properties properties) {
		return new TestBinding(name, inboundBindTarget, Binding.Type.consumer, messageCollector);
	}

	/**
	 * Registers a single subscriber to the channel, that enqueues messages for later retrieval and assertion in tests.
	 */
	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		final BlockingQueue<Message<?>> queue = messageCollector.register(outboundBindTarget);
		((SubscribableChannel)outboundBindTarget).subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				queue.add(message);
			}
		});
		this.messageChannels.put(name, outboundBindTarget);
		return new TestBinding(name, outboundBindTarget, Binding.Type.producer, messageCollector);
	}

	public MessageCollector messageCollector() {
		return messageCollector;
	}

	public MessageChannel getChannelForName(String name) {
		return this.messageChannels.get(name);
	}

	/**
	 * Maintains mappings between channels and queues.
	 *
	 * @author Eric Bottard
	 */
	private static class MessageCollectorImpl implements MessageCollector{

		private final Map<MessageChannel, BlockingQueue<Message<?>>> results = new HashMap<>();

		private BlockingQueue<Message<?>> register(MessageChannel channel) {
			LinkedBlockingDeque<Message<?>> result = new LinkedBlockingDeque<>();
			Assert.isTrue(!results.containsKey(channel), "Channel [" + channel + "] was already bound");
			results.put(channel, result);
			return result;
		}

		private void unregister(MessageChannel channel) {
			Assert.notNull(results.remove(channel), "Trying to unregister a mapping for an unknown channel [" + channel + "]");
		}

		@Override
		public BlockingQueue<Message<?>> forChannel(MessageChannel channel) {
			BlockingQueue<Message<?>> queue = results.get(channel);
			Assert.notNull(queue, "Channel [" + channel + "] was not bound by " + TestSupportBinder.class);
			return queue;
		}
	}

	/**
	 * @author Marius Bogoevici
	 */
	public static class TestBinding implements Binding<MessageChannel> {

		private final MessageChannel target;

		private final Type type;

		private final MessageCollectorImpl messageCollector;

		private String name;

		public TestBinding(String name, MessageChannel target, Type type, MessageCollectorImpl messageCollector) {
			this.name = name;
			this.target = target;
			this.type = type;
			this.messageCollector = messageCollector;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public MessageChannel getTarget() {
			return target;
		}

		@Override
		public Type getType() {
			return type;
		}

		@Override
		public void unbind() {
			messageCollector.unregister(target);
		}
	}
}
