/*
 * Copyright 2016-2018 the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;

/**
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 * @deprecated as of 2.0. Will be removed in 2.1
 */
@Deprecated
public class JavaSerializationMessageConverter extends AbstractMessageConverter {

	public JavaSerializationMessageConverter() {
		super(Arrays.asList(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT));
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		if (clazz != null) {
			return Serializable.class.isAssignableFrom(clazz);
		}
		return true;
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass,
			Object conversionHint) {
		if (!(message.getPayload() instanceof byte[])) {
			return null;
		}
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(
					(byte[]) (message.getPayload()));
			return new ObjectInputStream(bis).readObject();
		}
		catch (Exception e) {
			this.logger.error(e.getMessage(), e);
		}
		return null;
	}

	@Override
	protected Object convertToInternal(Object payload, MessageHeaders headers,
			Object conversionHint) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			new ObjectOutputStream(bos).writeObject(payload);
		}
		catch (IOException e) {
			this.logger.error(e.getMessage(), e);
			return null;
		}
		return bos.toByteArray();
	}

}
