/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.bus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.DefaultContentTypeResolver;
import org.springframework.util.MimeType;

/**
 * A {@link DefaultContentTypeResolver} that can parse String values.
 * 
 * @author David Turanski
 */
public class StringConvertingContentTypeResolver extends DefaultContentTypeResolver {

	private ConcurrentMap<String,MimeType> mimeTypeCache = new ConcurrentHashMap<>();

	@Override
	public MimeType resolve(MessageHeaders headers) {
		return resolve((Map<String, Object>) headers);
	}

	public MimeType resolve(Map<String,Object> headers) {
		Object value = headers.get(MessageHeaders.CONTENT_TYPE);
		if (value instanceof MimeType) {
			return (MimeType) value;
		}
		else if (value instanceof String) {
			MimeType mimeType = mimeTypeCache.get(value);
			if (mimeType == null) {
				String valueAsString = (String) value;
				mimeType = MimeType.valueOf(valueAsString);
				mimeTypeCache.put(valueAsString,mimeType);
			}
			return mimeType;
		}
		return getDefaultMimeType();
	}
}
