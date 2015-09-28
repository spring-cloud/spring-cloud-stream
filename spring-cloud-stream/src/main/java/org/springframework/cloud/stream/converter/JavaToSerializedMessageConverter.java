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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.springframework.messaging.Message;


/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * to convert from a POJO to byte[] with Java.io serialization if any.
 *
 * @author David Turanski
 */
public class JavaToSerializedMessageConverter extends AbstractFromMessageConverter {

	public JavaToSerializedMessageConverter() {
		super(MessageConverterUtils.X_JAVA_OBJECT, MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT);
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return new Class<?>[] { byte[].class };
	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return new Class<?>[] { Serializable.class };
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			new ObjectOutputStream(bos).writeObject(message.getPayload());
		}
		catch (IOException e) {
			logger.error(e.getMessage(), e);
			return null;
		}

		return buildConvertedMessage(bos.toByteArray(), message.getHeaders(),
				MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT);
	}

}
