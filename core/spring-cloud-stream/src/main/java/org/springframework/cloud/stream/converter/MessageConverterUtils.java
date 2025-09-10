/*
 * Copyright 2015-present the original author or authors.
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

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * Message conversion utility methods.
 *
 * @author David Turanski
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 */
public abstract class MessageConverterUtils {

	/**
	 * A general MimeType for Java Types.
	 */
	public static final MimeType X_JAVA_OBJECT = MimeType
			.valueOf("application/x-java-object");

	/**
	 * A general MimeType for a Java serialized byte array.
	 */
	public static final MimeType X_JAVA_SERIALIZED_OBJECT = MimeType
			.valueOf("application/x-java-serialized-object");

	/**
	 * Get the java Object type for the MimeType X_JAVA_OBJECT.
	 * @param contentType content type
	 * @return the class for the content type.
	 */
	public static Class<?> getJavaTypeForJavaObjectContentType(MimeType contentType) {
		Assert.isTrue(X_JAVA_OBJECT.includes(contentType), "Content type must be "
				+ X_JAVA_OBJECT.toString() + ", or " + "included in it");
		if (contentType.getParameter("type") != null) {
			try {
				return ClassUtils.forName(contentType.getParameter("type"), null);
			}
			catch (Exception e) {
				throw new ConversionException(e.getMessage(), e);
			}
		}
		return Object.class;
	}

	/**
	 * Build the conventional {@link MimeType} for a java object.
	 * @param clazz the java type
	 * @return the MIME type
	 */
	public static MimeType javaObjectMimeType(Class<?> clazz) {
		return MimeType.valueOf("application/x-java-object;type=" + clazz.getName());
	}

	public static MimeType getMimeType(String contentTypeString) {
		MimeType mimeType = null;
		if (StringUtils.hasText(contentTypeString)) {
			try {
				mimeType = resolveContentType(contentTypeString);
			}
			catch (ClassNotFoundException cfe) {
				throw new IllegalArgumentException(
						"Could not find the class required for " + contentTypeString,
						cfe);
			}
		}
		return mimeType;
	}

	public static MimeType resolveContentType(String type)
			throws ClassNotFoundException, LinkageError {
		if (!type.contains("/")) {
			Class<?> javaType = resolveJavaType(type);
			return javaObjectMimeType(javaType);
		}
		return MimeType.valueOf(type);
	}

	public static Class<?> resolveJavaType(String type)
			throws ClassNotFoundException, LinkageError {
		return ClassUtils.forName(type, null);
	}

}
