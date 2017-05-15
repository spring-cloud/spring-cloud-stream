/*
 * Copyright 2016 the original author or authors.
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

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * Message converter providing backwards compatibility for applications using an Java type
 * as input.
 *
 * @author Marius Bogoevici
 */
public class JsonUnmarshallingConverter extends AbstractMessageConverter {

	private final ObjectMapper objectMapper;

	protected JsonUnmarshallingConverter(ObjectMapper objectMapper) {
		super(MessageConverterUtils.X_JAVA_OBJECT);
		this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
	}

	@Override
	protected boolean supports(Class<?> aClass) {
		return true;
	}

	@Override
	protected boolean canConvertFrom(Message<?> message, Class<?> targetClass) {
		if ((message.getPayload() instanceof String) || (message.getPayload() instanceof byte[])) {
			return true;
		}
		return canConvertFromBasedOnContentTypeHeader(message);
	}

	private boolean canConvertFromBasedOnContentTypeHeader(Message<?> message) {
		Object contentTypeHeader = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
		if (contentTypeHeader instanceof String) {
			return MimeTypeUtils.APPLICATION_JSON.includes(MimeTypeUtils.parseMimeType((String) contentTypeHeader));
		}
		else if (contentTypeHeader instanceof MimeType) {
			return MimeTypeUtils.APPLICATION_JSON.includes((MimeType) contentTypeHeader);
		}
		else {
			return contentTypeHeader == null;
		}
	}

	@Override
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
		Object payload = message.getPayload();
		try {
			return payload instanceof byte[] ? objectMapper.readValue((byte[]) payload, targetClass)
					: objectMapper.readValue((String) payload, targetClass);
		}
		catch (IOException e) {
			throw new MessageConversionException("Cannot parse payload ", e);
		}
	}

	@Override
	protected Object convertToInternal(Object payload, MessageHeaders headers, Object conversionHint) {
		return super.convertToInternal(payload, headers, conversionHint);
	}
}
