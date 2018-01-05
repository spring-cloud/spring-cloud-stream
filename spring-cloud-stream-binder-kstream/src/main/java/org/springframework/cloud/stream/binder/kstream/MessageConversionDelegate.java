/*
 * Copyright 2018 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * @author Soby Chacko
 */
public class MessageConversionDelegate {

	private final BindingServiceProperties bindingServiceProperties;
	private final CompositeMessageConverterFactory compositeMessageConverterFactory;

	public MessageConversionDelegate(BindingServiceProperties bindingServiceProperties,
									CompositeMessageConverterFactory compositeMessageConverterFactory) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.compositeMessageConverterFactory = compositeMessageConverterFactory;
	}

	public KeyValueMapper<Object, Object, KeyValue<Object, Object>> outboundKeyValueMapper(String name) {
		BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(name);
		String contentType = bindingProperties.getContentType();
		MessageConverter messageConverter = StringUtils.hasText(contentType) ? compositeMessageConverterFactory
				.getMessageConverterForType(MimeType.valueOf(contentType))
				: null;

		return (k, v) -> {
			Message<?> message = v instanceof Message<?> ? (Message<?>)v :
					MessageBuilder.withPayload(v).build();
			Map<String, Object> headers = new HashMap<>(message.getHeaders());
			if (!StringUtils.isEmpty(contentType)) {
				headers.put(MessageHeaders.CONTENT_TYPE, contentType);
			}
			MessageHeaders messageHeaders = new MessageHeaders(headers);
			return new KeyValue<>(k,
					messageConverter.toMessage(message.getPayload(),
							messageHeaders).getPayload());
		};
	}

	@SuppressWarnings("unchecked")
	public KeyValueMapper<Object, Object, KeyValue<Object, Object>> inboundKeyValueMapper(Class<?> valueClass) {
		MessageConverter messageConverter = compositeMessageConverterFactory.getMessageConverterForAllRegistered();
		return (KeyValueMapper) (o, o2) -> {
			KeyValue<Object, Object> keyValue;
			if (valueClass.isAssignableFrom(o2.getClass())) {
				keyValue =  new KeyValue<>(o, o2);
			}
			else if (o2 instanceof Message) {
				if (valueClass.isAssignableFrom(((Message) o2).getPayload().getClass())) {
					keyValue = new KeyValue<>(o, ((Message) o2).getPayload());
				}
				else {
					keyValue = new KeyValue<>(o, messageConverter.fromMessage((Message) o2, valueClass));
				}
			}
			else if(o2 instanceof String || o2 instanceof byte[]) {
				Message<Object> message = MessageBuilder.withPayload(o2).build();
				keyValue =  new KeyValue<>(o, messageConverter.fromMessage(message, valueClass));
			}
			else {
				keyValue =  new KeyValue<>(o, o2);
			}
			return keyValue;
		};
	}
}
