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
import java.util.List;

import org.springframework.messaging.Message;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;


/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * to convert a String to a byte[], applying the provided Charset in
 * the content-type header if any.
 *
 * @author David Turanski
 */
public class StringToByteArrayMessageConverter extends AbstractFromMessageConverter {

	private final static ContentTypeResolver contentTypeResolver = new StringConvertingContentTypeResolver();

	private final static List<MimeType> targetMimeTypes = new ArrayList<MimeType>();
	static {
		targetMimeTypes.add(MimeTypeUtils.APPLICATION_OCTET_STREAM);
	}

	public StringToByteArrayMessageConverter() {
		super(targetMimeTypes);
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return new Class<?>[] { byte[].class };
	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return new Class<?>[] { String.class };
	}

	/**
	 * Don't need to manipulate message headers. Just return the payload
	 */
	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass) {
		MimeType mimeType = contentTypeResolver.resolve(message.getHeaders());
		byte[] converted = null;
		if (mimeType == null || mimeType.getParameter("Charset") == null) {
			converted = ((String) message.getPayload()).getBytes();
		}
		else {
			String encoding = mimeType.getParameter("Charset");
			if (encoding != null) {
				try {
					converted = ((String) message.getPayload()).getBytes(encoding);
				}
				catch (UnsupportedEncodingException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return converted;

	}
}
