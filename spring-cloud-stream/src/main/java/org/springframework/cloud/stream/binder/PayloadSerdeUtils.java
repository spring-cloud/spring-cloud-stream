/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.core.serializer.support.SerializationFailedException;
import org.springframework.integration.codec.Codec;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ObjectUtils;

/**
 * Utility class for serializing and de-serializing the message payload.
 *
 * @author Soby Chacko
 */
public abstract class PayloadSerdeUtils {

	protected static final Log LOGGER = LogFactory.getLog(PayloadSerdeUtils.class);

	/**
	 * Serialize the message payload unless it is a byte array.
	 *
	 * @param message the message with the payload to serialize
	 * @param codec the codec used for serialization
	 * @return the Message with teh serialized payload
	 */
	public static MessageValues serializePayloadIfNecessary(Message<?> message, Codec codec) {
		Object originalPayload = message.getPayload();
		Object originalContentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);

		// Pass content type as String since some transport adapters will exclude
		// CONTENT_TYPE Header otherwise
		Object contentType = JavaClassMimeTypeConversion
				.mimeTypeFromObject(originalPayload, ObjectUtils.nullSafeToString(originalContentType)).toString();
		Object payload = serializePayloadIfNecessary(originalPayload, codec);
		MessageValues messageValues = new MessageValues(message);
		messageValues.setPayload(payload);
		messageValues.put(MessageHeaders.CONTENT_TYPE, contentType);
		if (originalContentType != null && !originalContentType.toString().equals(contentType.toString())) {
			messageValues.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, originalContentType.toString());
		}
		return messageValues;
	}

	/**
	 * Serialize the payload object if it is not a byte array.
	 *
	 * @param originalPayload the payload to serialize
	 * @param codec the codec used for serialization
	 * @return the serialized byte array or the original payload if it is already a byte array
	 */
	public static byte[] serializePayloadIfNecessary(Object originalPayload, Codec codec) {
		if (originalPayload instanceof byte[]) {
			return (byte[]) originalPayload;
		}
		else {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try {
				if (originalPayload instanceof String) {
					return ((String) originalPayload).getBytes("UTF-8");
				}
				codec.encode(originalPayload, bos);
				return bos.toByteArray();
			}
			catch (IOException e) {
				throw new SerializationFailedException(
						"unable to serialize payload [" + originalPayload.getClass().getName() + "]", e);
			}
		}
	}

	/**
	 * De-serialize the message payload if necessary.
	 *
	 * @param messageValues message with the payload to deserialize
	 * @param contentTypeResolver used for resolving the mime type.
	 * @param payloadTypeCache Map used for caching the payload type
	 * @param codec used for deserialization
	 * @return Deserialized Message.
	 */
	public static MessageValues deserializePayloadIfNecessary(MessageValues messageValues, ContentTypeResolver contentTypeResolver,
															Map<String, Class<?>> payloadTypeCache, Codec codec) {
		Object originalPayload = messageValues.getPayload();
		MimeType contentType = contentTypeResolver.resolve(new MessageHeaders(messageValues.getHeaders()));
		Object payload = deserializePayload(originalPayload, contentType, payloadTypeCache, codec);
		if (payload != null) {
			messageValues.setPayload(payload);
			Object originalContentType = messageValues.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
			// Reset content-type only if the original content type is not null (when
			// receiving messages from
			// non-SCSt applications).
			if (originalContentType != null) {
				messageValues.put(MessageHeaders.CONTENT_TYPE, originalContentType);
				messageValues.remove(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
			}
		}
		return messageValues;
	}

	private static Object deserializePayload(Object payload, MimeType contentType,
											Map<String, Class<?>> payloadTypeCache, Codec codec) {
		if (payload instanceof byte[]) {
			if (contentType == null || MimeTypeUtils.APPLICATION_OCTET_STREAM.equals(contentType)) {
				return payload;
			}
			else {
				return deserializePayload((byte[]) payload, contentType, payloadTypeCache, codec);
			}
		}
		return payload;
	}

	private static Object deserializePayload(byte[] bytes, MimeType contentType,
									Map<String, Class<?>> payloadTypeCache, Codec codec) {
		if ("text".equalsIgnoreCase(contentType.getType()) || MimeTypeUtils.APPLICATION_JSON.equals(contentType)) {
			try {
				return new String(bytes, "UTF-8");
			}
			catch (UnsupportedEncodingException e) {
				String errorMessage = "unable to deserialize [java.lang.String]. Encoding not supported. "
						+ e.getMessage();
				PayloadSerdeUtils.LOGGER.error(errorMessage);
				throw new SerializationFailedException(errorMessage, e);
			}
		}
		else {
			String className = JavaClassMimeTypeConversion.classNameFromMimeType(contentType);
			try {
				// Cache types to avoid unnecessary ClassUtils.forName calls.
				Class<?> targetType = payloadTypeCache.get(className);
				if (targetType == null) {
					targetType = ClassUtils.forName(className, null);
					payloadTypeCache.put(className, targetType);
				}
				return codec.decode(bytes, targetType);
			} // catch all exceptions that could occur during de-serialization
			catch (Exception e) {
				String errorMessage = "Unable to deserialize [" + className + "] using the contentType [" + contentType
						+ "] " + e.getMessage();
				PayloadSerdeUtils.LOGGER.error(errorMessage);
				throw new SerializationFailedException(errorMessage, e);
			}
		}
	}

	/**
	 * Handles representing any java class as a {@link MimeType}.
	 *
	 * @author David Turanski
	 * @author Ilayaperumal Gopinathan
	 */
	public abstract static class JavaClassMimeTypeConversion {

		private static ConcurrentMap<String, MimeType> mimeTypesCache = new ConcurrentHashMap<>();

		static MimeType mimeTypeFromObject(Object payload, String originalContentType) {
			Assert.notNull(payload, "payload object cannot be null.");
			if (payload instanceof byte[]) {
				return MimeTypeUtils.APPLICATION_OCTET_STREAM;
			}
			if (payload instanceof String) {
				return MimeTypeUtils.APPLICATION_JSON_VALUE.equals(originalContentType) ? MimeTypeUtils.APPLICATION_JSON
						: MimeTypeUtils.TEXT_PLAIN;
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
						modifiedClassName = modifiedClassName.substring(0, modifiedClassName.length() - 1);
					}
					// Wrap in quotes to handle the illegal '[' character
					modifiedClassName = "\"" + modifiedClassName + "\"";
				}
				mimeType = MimeType.valueOf("application/x-java-object;type=" + modifiedClassName);
				mimeTypesCache.put(className, mimeType);
			}
			return mimeType;
		}

		static String classNameFromMimeType(MimeType mimeType) {
			Assert.notNull(mimeType, "mimeType cannot be null.");
			String className = mimeType.getParameter("type");
			if (className == null) {
				return null;
			}
			// unwrap quotes if any
			className = className.replace("\"", "");

			// restore trailing ';'
			if (className.contains("[L")) {
				className += ";";
			}
			return className;
		}
	}
}
