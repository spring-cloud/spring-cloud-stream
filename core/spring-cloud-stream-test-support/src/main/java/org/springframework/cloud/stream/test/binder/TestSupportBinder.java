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

package org.springframework.cloud.stream.test.binder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.cloud.stream.test.matcher.MessageQueueMatcher;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.converter.DefaultContentTypeResolver;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

/**
 * A minimal binder that
 * <ul>
 * <li>does nothing about binding consumers, leaving the channel as-is, so that a test
 * author can interact with it directly,</li>
 * <li>registers a queue channel on the producer side, so that it is easy to assert what
 * is received.</li>
 * </ul>
 *
 * @author Eric Bottard
 * @author Gary Russell
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @see MessageQueueMatcher
 */
public class TestSupportBinder
		implements Binder<MessageChannel, ConsumerProperties, ProducerProperties> {

	private final MessageCollectorImpl messageCollector = new MessageCollectorImpl();

	private final ConcurrentMap<String, MessageChannel> messageChannels = new ConcurrentHashMap<>();

	@Override
	public Binding<MessageChannel> bindConsumer(String name, String group,
			MessageChannel inboundBindTarget, ConsumerProperties properties) {
		return new TestBinding(inboundBindTarget, null);
	}

	/**
	 * Registers a single subscriber to the channel, that enqueues messages for later
	 * retrieval and assertion in tests.
	 */
	@Override
	public Binding<MessageChannel> bindProducer(String name,
			MessageChannel outboundBindTarget, ProducerProperties properties) {
		final BlockingQueue<Message<?>> queue = this.messageCollector
				.register(outboundBindTarget, properties.isUseNativeEncoding());
		((SubscribableChannel) outboundBindTarget).subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				queue.add(message);
			}
		});
		this.messageChannels.put(name, outboundBindTarget);
		return new TestBinding(outboundBindTarget, this.messageCollector);
	}

	public MessageCollector messageCollector() {
		return this.messageCollector;
	}

	public MessageChannel getChannelForName(String name) {
		return this.messageChannels.get(name);
	}

	/**
	 * Maintains mappings between channels and queues.
	 *
	 * @author Eric Bottard
	 */
	private static class MessageCollectorImpl implements MessageCollector {

		private final Map<MessageChannel, BlockingQueue<Message<?>>> results = new HashMap<>();

		private BlockingQueue<Message<?>> register(MessageChannel channel,
				boolean useNativeEncoding) {
			// we need to add this interceptor to ensure MessageCollector's compatibility
			// with
			// previous versions of SCSt when native encoding is disabled.
			if (!useNativeEncoding) {
				((AbstractMessageChannel) channel)
						.addInterceptor(new InboundMessageConvertingInterceptor());
			}
			LinkedBlockingDeque<Message<?>> result = new LinkedBlockingDeque<>();
			Assert.isTrue(!this.results.containsKey(channel),
					"Channel [" + channel + "] was already bound");
			this.results.put(channel, result);
			return result;
		}

		private void unregister(MessageChannel channel) {
			Assert.notNull(this.results.remove(channel),
					"Trying to unregister a mapping for an unknown channel [" + channel
							+ "]");
		}

		@Override
		public BlockingQueue<Message<?>> forChannel(MessageChannel channel) {
			BlockingQueue<Message<?>> queue = this.results.get(channel);
			Assert.notNull(queue, "Channel [" + channel + "] was not bound by "
					+ TestSupportBinder.class);
			return queue;
		}

	}

	/**
	 * @author Marius Bogoevici
	 */
	private static final class TestBinding implements Binding<MessageChannel> {

		private final MessageChannel target;

		private final MessageCollectorImpl messageCollector;

		private TestBinding(MessageChannel target,
				MessageCollectorImpl messageCollector) {
			this.target = target;
			this.messageCollector = messageCollector;
		}

		@Override
		public void unbind() {
			if (this.messageCollector != null) {
				this.messageCollector.unregister(this.target);
			}
		}

	}

	/**
	 * This is really an interceptor to maintain MessageCollector's backward compatibility
	 * with the behavior established in 1.3 - BINDER_ORIGINAL_CONTENT_TYPE - Kryo and Java
	 * deserialization - byte[] to String conversion - etc.
	 */
	private final static class InboundMessageConvertingInterceptor
			implements ChannelInterceptor {

		private final DefaultContentTypeResolver contentTypeResolver = new DefaultContentTypeResolver();

		private final CompositeMessageConverterFactory converterFactory = new CompositeMessageConverterFactory();

		/*
		 * Candidate to go into some utils class
		 */
		private static boolean equalTypeAndSubType(MimeType m1, MimeType m2) {
			return m1 != null && m2 != null && m1.getType().equalsIgnoreCase(m2.getType())
					&& m1.getSubtype().equalsIgnoreCase(m2.getSubtype());
		}

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			Class<?> targetClass = null;
			MessageConverter converter = null;
			MimeType contentType = MimeType.valueOf(this.contentTypeResolver
									.resolve(message.getHeaders()).toString());

			if (contentType != null) {
				if (equalTypeAndSubType(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT,
						contentType)
						|| equalTypeAndSubType(MessageConverterUtils.X_JAVA_OBJECT,
								contentType)) {
					// for Java and Kryo de-serialization we need to reset the content
					// type
					message = MessageBuilder.fromMessage(message)
							.setHeader(MessageHeaders.CONTENT_TYPE, contentType).build();
					converter = equalTypeAndSubType(
							MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT, contentType)
									? this.converterFactory
											.getMessageConverterForType(contentType)
									: this.converterFactory
											.getMessageConverterForAllRegistered();
					String targetClassName = contentType.getParameter("type");
					if (StringUtils.hasText(targetClassName)) {
						try {
							targetClass = Class.forName(targetClassName, false,
									Thread.currentThread().getContextClassLoader());
						}
						catch (Exception e) {
							throw new IllegalStateException(
									"Failed to determine class name for contentType: "
											+ message.getHeaders(),
									e);
						}
					}
				}

			}

			Object payload;
			if (converter != null) {
				Assert.isTrue(
						!(equalTypeAndSubType(MessageConverterUtils.X_JAVA_OBJECT,
								contentType) && targetClass == null),
						"Cannot deserialize into message since 'contentType` is not "
								+ "encoded with the actual target type."
								+ "Consider 'application/x-java-object; type=foo.bar.MyClass'");
				payload = converter.fromMessage(message, targetClass);
			}
			else {
				MimeType deserializeContentType = this.contentTypeResolver
						.resolve(message.getHeaders());
				if (deserializeContentType == null) {
					deserializeContentType = contentType;
				}
				payload = deserializeContentType == null ? message.getPayload() : this
						.deserializePayload(message.getPayload(), deserializeContentType);
			}
			message = MessageBuilder.withPayload(payload)
					.copyHeaders(message.getHeaders())
					.setHeader(MessageHeaders.CONTENT_TYPE, contentType)
					.build();
			return message;
		}

		private Object deserializePayload(Object payload, MimeType contentType) {
			if (payload instanceof byte[]
					&& ("text".equalsIgnoreCase(contentType.getType())
							|| equalTypeAndSubType(MimeTypeUtils.APPLICATION_JSON,
									contentType))) {
				payload = new String((byte[]) payload, StandardCharsets.UTF_8);
			}
			return payload;
		}

	}

}
