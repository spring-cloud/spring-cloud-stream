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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.messaging.Message;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;


/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * to convert from byte[] to String applying the Charset provided in
 * the content-type header if any.
 *
 * @author David Turanski
 */
public class ByteArrayToStringMessageConverter extends AbstractFromMessageConverter {

	private final static ContentTypeResolver contentTypeResolver = new StringConvertingContentTypeResolver();

	private final static List<MimeType> targetMimeTypes = new ArrayList<MimeType>();
	static {
		targetMimeTypes.add(MessageConverterUtils.X_SPRING_STRING);
		targetMimeTypes.add(MessageConverterUtils.X_JAVA_OBJECT);
		targetMimeTypes.add(MimeTypeUtils.TEXT_PLAIN);
	}

	public ByteArrayToStringMessageConverter() {
		super(Arrays.asList(new MimeType[] { MimeTypeUtils.APPLICATION_OCTET_STREAM, MimeTypeUtils.TEXT_PLAIN }),
				targetMimeTypes, contentTypeResolver);
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return new Class<?>[] { String.class };
	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return new Class<?>[] { byte[].class };
	}

	/**
	 * Don't need to manipulate message headers. Just return payload
	 */
	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass) {
		MimeType mimeType = contentTypeResolver.resolve(message.getHeaders());

		String converted = null;

		if (mimeType == null || mimeType.getParameter("Charset") == null) {
			converted = new String((byte[]) message.getPayload());
		}
		else {
			String encoding = mimeType.getParameter("Charset");
			if (encoding != null) {
				try {
					converted = new String((byte[]) message.getPayload(), encoding);
				}
				catch (UnsupportedEncodingException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return converted;

	}
}
