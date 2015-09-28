/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.messaging.Message;
import org.springframework.util.MimeTypeUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


/**
 *
 * @author David Turanski
 */
public class JsonToPojoMessageConverter extends AbstractFromMessageConverter {

	private final ObjectMapper mapper = new ObjectMapper();

	public JsonToPojoMessageConverter() {
		super(MimeTypeUtils.APPLICATION_JSON, MessageConverterUtils.X_JAVA_OBJECT);
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return new Class<?>[] {String.class, byte[].class};
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return null; // any type
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass) {
		Object result = null;
		try {
			Object payload = message.getPayload();

			if (payload instanceof byte[]) {
				result = mapper.readValue((byte[]) payload, targetClass);
			}
			else if (payload instanceof String) {
				result = mapper.readValue((String) payload, targetClass);
			}
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;
		}
		return buildConvertedMessage(result, message.getHeaders(),
				MessageConverterUtils.javaObjectMimeType(targetClass));
	}
}
