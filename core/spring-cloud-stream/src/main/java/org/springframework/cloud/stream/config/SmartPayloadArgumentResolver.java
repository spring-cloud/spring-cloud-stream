/*
 * Copyright 2016-present the original author or authors.
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

import org.springframework.core.MethodParameter;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.BindingResult;
import org.springframework.validation.ObjectError;
import org.springframework.validation.Validator;

/**
 * @author Oleg Zhurakousky
 * @author Gary Russell
 */
class SmartPayloadArgumentResolver extends PayloadMethodArgumentResolver {

	private final MessageConverter messageConverter;

	SmartPayloadArgumentResolver(MessageConverter messageConverter) {
		super(messageConverter);
		this.messageConverter = messageConverter;
	}

	SmartPayloadArgumentResolver(MessageConverter messageConverter, Validator validator) {
		super(messageConverter, validator, true);
		this.messageConverter = messageConverter;
	}

	SmartPayloadArgumentResolver(MessageConverter messageConverter, Validator validator,
			boolean useDefaultResolution) {
		super(messageConverter, validator, useDefaultResolution);
		this.messageConverter = messageConverter;
	}

	@Override
	public boolean supportsParameter(MethodParameter parameter) {
		return (!Message.class.isAssignableFrom(parameter.getParameterType())
				&& !MessageHeaders.class.isAssignableFrom(parameter.getParameterType())
				&& !parameter.hasParameterAnnotation(Header.class)
				&& !parameter.hasParameterAnnotation(Headers.class));
	}

	/**
	 * Needed to support mapping KafkaNull to null in method invocation.
	 */
	@Override
	protected boolean isEmptyPayload(Object payload) {
		return super.isEmptyPayload(payload)
				|| "org.springframework.kafka.support.KafkaNull"
						.equals(payload.getClass().getName());
	}

	@Override
	@Nullable
	public Object resolveArgument(MethodParameter parameter, Message<?> message)
			throws Exception {
		Payload ann = parameter.getParameterAnnotation(Payload.class);
		if (ann != null && StringUtils.hasText(ann.expression())) {
			throw new IllegalStateException(
					"@Payload SpEL expressions not supported by this resolver");
		}

		Object payload = message.getPayload();
		if (isEmptyPayload(payload)) {
			if (ann == null || ann.required()) {
				String paramName = getParameterName(parameter);
				BindingResult bindingResult = new BeanPropertyBindingResult(payload,
						paramName);
				bindingResult.addError(
						new ObjectError(paramName, "Payload value must not be empty"));
				throw new MethodArgumentNotValidException(message, parameter,
						bindingResult);
			}
			else {
				return null;
			}
		}

		Class<?> targetClass = parameter.getParameterType();
		Class<?> payloadClass = payload.getClass();
		if (conversionNotRequired(payloadClass, targetClass)) {
			validate(message, parameter, payload);
			return payload;
		}
		else {
			if (this.messageConverter instanceof SmartMessageConverter) {
				SmartMessageConverter smartConverter = (SmartMessageConverter) this.messageConverter;
				payload = smartConverter.fromMessage(message, targetClass, parameter);
			}
			else {
				payload = this.messageConverter.fromMessage(message, targetClass);
			}
			if (payload == null) {
				throw new MessageConversionException(message,
						"Cannot convert from [" + payloadClass.getName() + "] to ["
								+ targetClass.getName() + "] for " + message);
			}
			validate(message, parameter, payload);
			return payload;
		}
	}

	private boolean conversionNotRequired(Class<?> a, Class<?> b) {
		return b == Object.class
				? ClassUtils.isAssignable(a, b) : ClassUtils.isAssignable(b, a);
	}

	private String getParameterName(MethodParameter param) {
		String paramName = param.getParameterName();
		return (paramName != null ? paramName : "Arg " + param.getParameterIndex());
	}

}
