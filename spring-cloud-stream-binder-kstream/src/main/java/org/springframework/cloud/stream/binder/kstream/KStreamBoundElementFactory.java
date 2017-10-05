/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kstream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 */
public class KStreamBoundElementFactory extends AbstractBindingTargetFactory<KStream> {

	private final KStreamBuilder kStreamBuilder;

	private final BindingServiceProperties bindingServiceProperties;

	private CompositeMessageConverterFactory compositeMessageConverterFactory;

	public KStreamBoundElementFactory(KStreamBuilder streamBuilder, BindingServiceProperties bindingServiceProperties,
									CompositeMessageConverterFactory compositeMessageConverterFactory) {
		super(KStream.class);
		this.bindingServiceProperties = bindingServiceProperties;
		this.kStreamBuilder = streamBuilder;
		this.compositeMessageConverterFactory = compositeMessageConverterFactory;
	}

	@Override
	public KStream createInput(String name) {
		KStream<Object, Object> stream = kStreamBuilder.stream(bindingServiceProperties.getBindingDestination(name));
		stream = stream.map((key, value) -> {
			KeyValue<Object, Object> keyValue;
			BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(name);
			String contentType = bindingProperties.getContentType();
			if (!StringUtils.isEmpty(contentType)) {
				Message<Object> message = MessageBuilder.withPayload(value)
						.setHeader(MessageHeaders.CONTENT_TYPE, contentType).build();
				keyValue = new KeyValue<>(key, message);
			}
			else {
				keyValue = new KeyValue<>(key, value);
			}
			return keyValue;
		});
		return stream;
	}

	@Override
	@SuppressWarnings("unchecked")
	public KStream createOutput(final String name) {
		BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(name);
		String contentType = bindingProperties.getContentType();
		MessageConverter messageConverter = StringUtils.hasText(contentType) ? compositeMessageConverterFactory
				.getMessageConverterForType(MimeType.valueOf(contentType)) : null;
		KStreamWrapperHandler handler = new KStreamWrapperHandler(messageConverter, bindingServiceProperties, name);
		ProxyFactory proxyFactory = new ProxyFactory(KStreamWrapper.class, KStream.class);
		proxyFactory.addAdvice(handler);
		return (KStream) proxyFactory.getProxy();
	}

	public interface KStreamWrapper {

		void wrap(KStream<Object, Object> delegate);
	}

	private static class KStreamWrapperHandler implements KStreamWrapper, MethodInterceptor {

		private KStream<Object, Object> delegate;

		private final MessageConverter messageConverter;
		private final BindingServiceProperties bindingServiceProperties;
		private String name;

		KStreamWrapperHandler(MessageConverter messageConverter,
									BindingServiceProperties bindingServiceProperties,
									String name) {
			this.messageConverter = messageConverter;
			this.bindingServiceProperties = bindingServiceProperties;
			this.name = name;
		}

		public void wrap(KStream<Object, Object> delegate) {
			Assert.notNull(delegate, "delegate cannot be null");
			Assert.isNull(this.delegate, "delegate already set to " + this.delegate);
			if (messageConverter != null) {
				KeyValueMapper<Object, Object, KeyValue<Object, Object>> keyValueMapper = (k, v) -> {
					Message<?> message = (Message<?>) v;
					BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(name);
					String contentType = bindingProperties.getContentType();
					MutableMessageHeaders messageHeaders = new MutableMessageHeaders(((Message<?>) v).getHeaders());
					if (!StringUtils.isEmpty(contentType)) {
						messageHeaders.put(MessageHeaders.CONTENT_TYPE, contentType);
					}
					return new KeyValue<>(k,
							messageConverter.toMessage(message.getPayload(),
									messageHeaders));
				};
				delegate = delegate.map(keyValueMapper);
			}
			this.delegate = delegate;
		}

		@Override
		public Object invoke(MethodInvocation methodInvocation) throws Throwable {
			if (methodInvocation.getMethod().getDeclaringClass().equals(KStream.class)) {
				Assert.notNull(delegate, "Trying to invoke " + methodInvocation
						.getMethod() + "  but no delegate has been set.");
				return methodInvocation.getMethod().invoke(delegate, methodInvocation.getArguments());
			}
			else if (methodInvocation.getMethod().getDeclaringClass().equals(KStreamWrapper.class)) {
				return methodInvocation.getMethod().invoke(this, methodInvocation.getArguments());
			}
			else {
				throw new IllegalStateException("Only KStream method invocations are permitted");
			}
		}
	}
}
