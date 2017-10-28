/*
 * Copyright 2017 the original author or authors.
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

import org.springframework.core.serializer.support.SerializationFailedException;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ObjectUtils;

/**
 * Utility class for serializing and de-serializing the message payload.
 *
 * @author Soby Chacko
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
public abstract class MessageSerializationUtils {

	private static final Map<String, Class<?>> payloadTypeCache = new ConcurrentHashMap<>();

	/**
	 * Serialize the message payload unless it is a byte array.
	 *
	 * @param message the message with the payload to serialize
	 * @return the Message with the serialized payload
	 */
	public static MessageValues serializePayload(Message<?> message) {
		Object originalPayload = message.getPayload();
		Object originalContentType = message.getHeaders()
				.get(MessageHeaders.CONTENT_TYPE);
		// Pass content type as String since some transport adapters will exclude
		// CONTENT_TYPE Header otherwise
		Object contentType = JavaClassMimeTypeUtils.mimeTypeFromObject(originalPayload,
				ObjectUtils.nullSafeToString(originalContentType)).toString();
		Object payload = serializePayload(originalPayload);
		MessageValues messageValues = new MessageValues(message);
		messageValues.setPayload(payload);
		messageValues.put(MessageHeaders.CONTENT_TYPE, contentType);
		if (originalContentType != null && !originalContentType.toString().equals(contentType.toString())) {
			messageValues.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, originalContentType.toString());
		}
		return messageValues;
	}


	/**
	 * De-serialize the message payload if necessary.
	 *
	 * @param messageValues message with the payload to deserialize
	 * @param contentTypeResolver used for resolving the mime type.
	 * @param codec used for deserialization
	 * @return Deserialized Message.
	 */
	public static MessageValues deserializePayload(MessageValues messageValues, ContentTypeResolver contentTypeResolver) {
		Object originalPayload = messageValues.getPayload();
		MimeType contentType = contentTypeResolver.resolve(new MessageHeaders(messageValues.getHeaders()));
		Object payload = deserializePayload(originalPayload, contentType);
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

	/**
	 * Serialize the payload object if it is not a byte array.
	 *
	 * @param originalPayload the payload to serialize
	 * @param codec the codec used for serialization
	 * @return the serialized byte array or the original payload if it is already a byte
	 * array
	 * @throws SerializationFailedException thrown when serialization failed
	 */
	public static byte[] serializePayload(Object originalPayload) {
		if (originalPayload instanceof byte[]) {
			return (byte[]) originalPayload;
		}
		else {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try {
				if (originalPayload instanceof String) {
					return ((String) originalPayload).getBytes("UTF-8");
				}
				return bos.toByteArray();
			}
			catch (IOException e) {
				throw new SerializationFailedException("unable to serialize payload ["
						+ originalPayload.getClass().getName() + "]", e);
			}
		}
	}

	private static Object deserializePayload(Object payload, MimeType contentType) {
		if (payload instanceof byte[]) {
			if (contentType == null || MimeTypeUtils.APPLICATION_OCTET_STREAM.equals(contentType)) {
				return payload;
			}
			else {
				return deserializePayload((byte[]) payload, contentType, payloadTypeCache);
			}
		}
		return payload;
	}

	private static Object deserializePayload(byte[] bytes, MimeType contentType, Map<String, Class<?>> payloadTypeCache) {
		if ("text".equalsIgnoreCase(contentType.getType())
				|| MimeTypeUtils.APPLICATION_JSON.equals(contentType)) {
			try {
				return new String(bytes, "UTF-8");
			}
			catch (UnsupportedEncodingException e) {
				String errorMessage = "unable to deserialize [java.lang.String]. Encoding not supported. "
						+ e.getMessage();
				throw new SerializationFailedException(errorMessage, e);
			}
		}
		return bytes;
	}
}
