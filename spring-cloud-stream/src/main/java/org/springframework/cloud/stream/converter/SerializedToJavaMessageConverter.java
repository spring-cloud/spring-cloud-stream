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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.springframework.messaging.Message;


/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * to deserialize {@link Serializable} Java objects.
 *
 * @author David Turanski
 */
public class SerializedToJavaMessageConverter extends AbstractFromMessageConverter {

	public SerializedToJavaMessageConverter() {
		super(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT, MessageConverterUtils.X_JAVA_OBJECT);
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return new Class<?>[] { Serializable.class };
	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return new Class<?>[] { byte[].class };
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass) {
		ByteArrayInputStream bis = new ByteArrayInputStream((byte[]) (message.getPayload()));
		Object result = null;
		try {
			result = new ObjectInputStream(bis).readObject();
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;
		}

		return buildConvertedMessage(result, message.getHeaders(),
				MessageConverterUtils.javaObjectMimeType(targetClass));
	}
}
