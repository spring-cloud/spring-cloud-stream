/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.lang.reflect.Type;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MethodArgumentTypeMismatchException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * @author Oleg Zhurakousky
 */
class SmartMessageMethodArgumentResolver extends MessageMethodArgumentResolver {

	private final MessageConverter messageConverter;

	SmartMessageMethodArgumentResolver() {
		this(null);
	}

	/**
	 * Create a resolver instance with the given {@link MessageConverter}.
	 * @param converter the MessageConverter to use (may be {@code null})
	 * @since 4.3
	 */
	SmartMessageMethodArgumentResolver(@Nullable MessageConverter converter) {
		this.messageConverter = converter;
	}

	@Override
	public Object resolveArgument(MethodParameter parameter, Message<?> message)
			throws Exception {
		Class<?> targetMessageType = parameter.getParameterType();
		Class<?> targetPayloadType = getPayloadType(parameter);

		if (!targetMessageType.isAssignableFrom(message.getClass())) {
			throw new MethodArgumentTypeMismatchException(message, parameter,
					"Actual message type '" + ClassUtils.getDescriptiveType(message)
							+ "' does not match expected type '"
							+ ClassUtils.getQualifiedName(targetMessageType) + "'");
		}

		Class<?> payloadClass = message.getPayload().getClass();

		if (message instanceof ErrorMessage
				|| conversionNotRequired(payloadClass, targetPayloadType)) {
			return message;
		}
		Object payload = message.getPayload();
		if (isEmptyPayload(payload)) {
			if (isExplicitNullPayload(payload)) {
				return message;
			}
			throw new MessageConversionException(message,
					"Cannot convert from actual payload type '"
							+ ClassUtils.getDescriptiveType(payload)
							+ "' to expected payload type '"
							+ ClassUtils.getQualifiedName(targetPayloadType)
							+ "' when payload is empty");
		}

		payload = convertPayload(message, parameter, targetPayloadType);
		return MessageBuilder.createMessage(payload, message.getHeaders());
	}

	private boolean conversionNotRequired(Class<?> a, Class<?> b) {
		return b == Object.class
				? ClassUtils.isAssignable(a, b) : ClassUtils.isAssignable(b, a);
	}

	private Class<?> getPayloadType(MethodParameter parameter) {
		Type genericParamType = parameter.getGenericParameterType();
		ResolvableType resolvableType = ResolvableType.forType(genericParamType)
				.as(Message.class);
		return resolvableType.getGeneric().toClass();
	}

	@Override
	protected boolean isEmptyPayload(@Nullable Object payload) {
		if (payload == null) {
			return true;
		}
		else if (payload instanceof byte[]) {
			return ((byte[]) payload).length == 0;
		}
		else if (payload instanceof String) {
			return !StringUtils.hasText((String) payload);
		}
		else {
			return "org.springframework.kafka.support.KafkaNull".equals(payload.getClass().getName());
		}
	}

	protected boolean isExplicitNullPayload(@Nullable Object payload) {
		return "org.springframework.kafka.support.KafkaNull"
						.equals(payload.getClass().getName());
	}

	private Object convertPayload(Message<?> message, MethodParameter parameter,
			Class<?> targetPayloadType) {
		Object result = null;
		if (this.messageConverter instanceof SmartMessageConverter) {
			SmartMessageConverter smartConverter = (SmartMessageConverter) this.messageConverter;
			result = smartConverter.fromMessage(message, targetPayloadType, parameter);
		}
		else if (this.messageConverter != null) {
			result = this.messageConverter.fromMessage(message, targetPayloadType);
		}

		if (result == null) {
			throw new MessageConversionException(message,
					"No converter found from actual payload type '"
							+ ClassUtils.getDescriptiveType(message.getPayload())
							+ "' to expected payload type '"
							+ ClassUtils.getQualifiedName(targetPayloadType) + "'");
		}
		return result;
	}

}
