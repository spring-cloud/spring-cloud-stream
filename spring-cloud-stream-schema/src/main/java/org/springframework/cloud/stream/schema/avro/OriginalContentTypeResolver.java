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

package org.springframework.cloud.stream.schema.avro;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.util.MimeType;

/**
 * @author Vinicius Carvalho
 *
 * Resolves contentType looking for a originalContentType header first. If not found
 * returns the contentType
 *
 */
class OriginalContentTypeResolver implements ContentTypeResolver {

	private ConcurrentMap<String, MimeType> mimeTypeCache = new ConcurrentHashMap<>();

	@Override
	public MimeType resolve(MessageHeaders headers) {
		Object contentType = headers
				.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE) != null
						? headers.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)
						: headers.get(MessageHeaders.CONTENT_TYPE);
		MimeType mimeType = null;
		if (contentType instanceof MimeType) {
			mimeType = (MimeType) contentType;
		}
		else if (contentType instanceof String) {
			mimeType = this.mimeTypeCache.get(contentType);
			if (mimeType == null) {
				String valueAsString = (String) contentType;
				mimeType = MimeType.valueOf(valueAsString);
				this.mimeTypeCache.put(valueAsString, mimeType);
			}
		}
		return mimeType;
	}

}
