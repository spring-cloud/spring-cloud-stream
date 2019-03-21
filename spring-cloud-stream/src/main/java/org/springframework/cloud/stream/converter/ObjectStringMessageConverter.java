/*
 * Copyright 2017-2018 the original author or authors.
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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.util.MimeType;

/**
 * A {@link org.springframework.messaging.converter.MessageConverter} to convert a
 * non-String objects to a String, when expected content type is "text/plain".
 *
 * It only performs conversions to internal format and is a wrapper around
 * {@link Object#toString()}.
 *
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 * @since 1.2
 */
public class ObjectStringMessageConverter extends AbstractMessageConverter {

	public ObjectStringMessageConverter() {
		super(new MimeType("text", "*", Charset.forName("UTF-8")));
		setStrictContentTypeMatch(true);
	}

	protected boolean supports(Class<?> clazz) {
		return true;
	}

	@Override
	protected boolean canConvertFrom(Message<?> message, Class<?> targetClass) {
		// only supports the conversion to String
		return supportsMimeType(message.getHeaders());
	}

	@Override
	protected boolean supportsMimeType(@Nullable MessageHeaders headers) {
		MimeType mimeType = getMimeType(headers);
		if (mimeType != null) {
			for (MimeType current : getSupportedMimeTypes()) {
				if (current.getType().equals(mimeType.getType())) {
					return true;
				}
			}
		}

		return super.supportsMimeType(headers);
	}

	protected Object convertFromInternal(Message<?> message, Class<?> targetClass,
			Object conversionHint) {
		if (message.getPayload() != null) {
			if (message.getPayload() instanceof byte[]) {
				if (byte[].class.isAssignableFrom(targetClass)) {
					return message.getPayload();
				}
				else {
					return new String((byte[]) message.getPayload(),
							StandardCharsets.UTF_8);
				}
			}
			else {
				if (byte[].class.isAssignableFrom(targetClass)) {
					return message.getPayload().toString()
							.getBytes(StandardCharsets.UTF_8);
				}
				else {
					return message.getPayload();
				}
			}
		}
		return null;
	}

	protected Object convertToInternal(Object payload, MessageHeaders headers,
			Object conversionHint) {
		if (payload != null) {
			if ((payload instanceof byte[])) {
				return payload;
			}
			else {
				return payload.toString().getBytes();
			}
		}
		else {
			return null;
		}
	}

}
