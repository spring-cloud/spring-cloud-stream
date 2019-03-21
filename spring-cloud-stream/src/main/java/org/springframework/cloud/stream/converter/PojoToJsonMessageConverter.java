/*
 * Copyright 2015-2016 the original author or authors.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.util.MimeTypeUtils;


/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * to convert a Java object to a JSON String
 *
 * @author David Turanski
 * @author David Liu
 * @author Ilayaperumal Gopinathan
 */
public class PojoToJsonMessageConverter extends AbstractFromMessageConverter {

	private final ObjectMapper objectMapper;

	@Value("${typeconversion.json.prettyPrint:false}")
	private volatile boolean prettyPrint;

	public PojoToJsonMessageConverter(ObjectMapper objectMapper) {
		super(MimeTypeUtils.APPLICATION_JSON);
		this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return new Class<?>[] {String.class};
	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return null;
	}


	public void setPrettyPrint(boolean prettyPrint) {
		this.prettyPrint = prettyPrint;
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
		Object result;
		try {
			if (prettyPrint) {
				result = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(message.getPayload());
			}
			else {
				result = objectMapper.writeValueAsString(message.getPayload());
			}
		}
		catch (JsonProcessingException e) {
			logger.error(e.getMessage(), e);
			return null;
		}
		return result;
	}
}
