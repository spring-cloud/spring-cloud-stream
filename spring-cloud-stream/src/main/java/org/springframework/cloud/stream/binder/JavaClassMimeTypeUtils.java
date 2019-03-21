/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * Handles representing any java class as a {@link MimeType}.
 *
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Soby Chacko
 */
public abstract class JavaClassMimeTypeUtils {

	private static ConcurrentMap<String, MimeType> mimeTypesCache = new ConcurrentHashMap<>();

	/**
	 * Convert payload to {@link MimeType} based on the content type on the message.
	 * @param payload the payload to convert
	 * @param originalContentType content type on the message
	 * @return converted {@link MimeType}
	 */
	public static MimeType mimeTypeFromObject(Object payload,
			String originalContentType) {
		Assert.notNull(payload, "payload object cannot be null.");
		if (payload instanceof byte[]) {
			return MimeTypeUtils.APPLICATION_OCTET_STREAM;
		}
		if (payload instanceof String) {
			return MimeTypeUtils.APPLICATION_JSON_VALUE.equals(originalContentType)
					? MimeTypeUtils.APPLICATION_JSON : MimeTypeUtils.TEXT_PLAIN;
		}
		String className = payload.getClass().getName();
		MimeType mimeType = mimeTypesCache.get(className);
		if (mimeType == null) {
			String modifiedClassName = className;
			if (payload.getClass().isArray()) {
				// Need to remove trailing ';' for an object array, e.g.
				// "[Ljava.lang.String;" or multi-dimensional
				// "[[[Ljava.lang.String;"
				if (modifiedClassName.endsWith(";")) {
					modifiedClassName = modifiedClassName.substring(0,
							modifiedClassName.length() - 1);
				}
				// Wrap in quotes to handle the illegal '[' character
				modifiedClassName = "\"" + modifiedClassName + "\"";
			}
			mimeType = MimeType
					.valueOf("application/x-java-object;type=" + modifiedClassName);
			mimeTypesCache.put(className, mimeType);
		}
		return mimeType;
	}

}
