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

package org.springframework.cloud.stream.converter;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.messaging.Message;
import org.springframework.util.MimeTypeUtils;

/**
 *
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 */
public class JsonToPojoMessageConverter extends AbstractFromMessageConverter {

	private final ObjectMapper objectMapper;

	public JsonToPojoMessageConverter(ObjectMapper objectMapper) {
		super(MimeTypeUtils.APPLICATION_JSON, MessageConverterUtils.X_JAVA_OBJECT);
		this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
	}

	@Override
	public Class<?>[] supportedPayloadTypes() {
		return new Class<?>[] {String.class, byte[].class};
	}

	@Override
	public Class<?>[] supportedTargetTypes() {
		return null; // any type
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
		Object result = null;
		try {
			Object payload = message.getPayload();

			if (payload instanceof byte[]) {
				result = objectMapper.readValue((byte[]) payload, targetClass);
			}
			else if (payload instanceof String) {
				result = objectMapper.readValue((String) payload, targetClass);
			}
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;
		}
		return result;
	}
}
