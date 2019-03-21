/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.converter;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.core.MethodParameter;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConversionException;

/**
 * Variation of {@link MappingJackson2MessageConverter} to support marshalling and
 * unmarshalling of Messages's payload from 'String' or 'byte[]' to an instance of a 'targetClass'
 * and and back to 'byte[]'
 *
 *
 * @author Oleg Zhurakousky
 * @author Gary Russell
 * @since 2.0
 *
 */
class ApplicationJsonMessageMarshallingConverter extends MappingJackson2MessageConverter {

	private final Map<ParameterizedTypeReference<?>, JavaType> typeCache = new ConcurrentHashMap<>();

	ApplicationJsonMessageMarshallingConverter(@Nullable ObjectMapper objectMapper) {
		if (objectMapper != null) {
			this.setObjectMapper(objectMapper);
		}
	}

	@Override
	protected Object convertToInternal(Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {
		if (payload instanceof byte[]) {
			return payload;
		}
		else if (payload instanceof String) {
			return ((String)payload).getBytes(StandardCharsets.UTF_8);
		}
		else {
			return super.convertToInternal(payload, headers, conversionHint);
		}
	}

	@Override
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
		Object result = null;
		if (conversionHint instanceof MethodParameter) {
			Class<?> conversionHintType = ((MethodParameter)conversionHint).getParameterType();
			if (Message.class.isAssignableFrom(conversionHintType)) {
				/*
				 * Ensures that super won't attempt to create Message as a result of conversion
				 * and stays at payload conversion only.
				 * The Message will eventually be created in MessageMethodArgumentResolver.resolveArgument(..)
				 */
				conversionHint = null;
			}
			else if (((MethodParameter)conversionHint).getGenericParameterType() instanceof ParameterizedType) {
				ParameterizedTypeReference<Object> forType = ParameterizedTypeReference.forType(((MethodParameter)conversionHint).getGenericParameterType());
				result = convertParameterizedType(message, targetClass, forType);
			}
		}
		else if (conversionHint instanceof ParameterizedTypeReference) {
			result = convertParameterizedType(message, targetClass, (ParameterizedTypeReference<?>)conversionHint);
		}

		if (result == null) {
			if (message.getPayload() instanceof byte[] &&  targetClass.isAssignableFrom(String.class)) {
				result = new String((byte[])message.getPayload(), StandardCharsets.UTF_8);
			}
			else {
				result = super.convertFromInternal(message, targetClass, conversionHint);
			}
		}

		return result;
	}

	private Object convertParameterizedType(Message<?> message, Class<?> targetClass, ParameterizedTypeReference<?> conversionHint) {
		ObjectMapper objectMapper = this.getObjectMapper();
		Object payload = message.getPayload();
		try {
			JavaType type = this.typeCache.get(conversionHint);
			if (type == null) {
				type = objectMapper.getTypeFactory().constructType((conversionHint).getType());
				this.typeCache.put(conversionHint, type);
			}
			if (payload instanceof byte[]) {
				return objectMapper.readValue((byte[]) payload, type);
			}
			else if (payload instanceof String) {
				return objectMapper.readValue((String) payload, type);
			}
			else {
				return null;
			}
		}
		catch (IOException e) {
			throw new MessageConversionException("Cannot parse payload ", e);
		}
	}
}
