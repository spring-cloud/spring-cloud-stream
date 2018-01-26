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

package org.springframework.cloud.stream.converter;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.SmartMessageConverter;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class SmartJsonMessageConverter implements SmartMessageConverter {

	private final ObjectMapper objectMapper;


	public SmartJsonMessageConverter() {
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public Object fromMessage(Message<?> message, Class<?> targetClass) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Message<?> toMessage(Object payload, MessageHeaders headers) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
		Object payload = message.getPayload();
		try {
			if (conversionHint instanceof ParameterizedTypeReference) {
				if (payload instanceof byte[]) {
					return this.objectMapper.readValue((byte[]) payload,
							this.objectMapper.getTypeFactory().constructType(
									((ParameterizedTypeReference<?>) conversionHint).getType()));
				}
				else if (payload instanceof String) {
					return this.objectMapper.readValue((String) payload,
							this.objectMapper.getTypeFactory().constructType(
									((ParameterizedTypeReference<?>) conversionHint).getType()));
				}
				else {
					throw new IllegalArgumentException("Unsupported payload type");
				}
			}
			else {
				throw new IllegalArgumentException("Must provide a ParamterizedTypeReference");
			}
		}
		catch (IOException e) {
			throw new MessageConversionException("Cannot parse payload ", e);
		}
	}

	@Override
	public Message<?> toMessage(Object payload, MessageHeaders headers, Object conversionHint) {
		throw new UnsupportedOperationException();
	}

}
