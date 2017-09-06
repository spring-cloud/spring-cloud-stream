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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.util.MimeType;

/**
 * Utility class for serializing and de-serializing the message payload.
 *
 * @author Soby Chacko
 * @author Vinicius Carvalho
 */
public abstract class MessageSerializationUtils {

	private static final Map<String, Class<?>> payloadTypeCache = new ConcurrentHashMap<>();

	/**
	 * Serialize the message payload unless it is a byte array.
	 *
	 * @param message the message with the payload to serialize
	 * @return the Message with teh serialized payload
	 */
	public static MessageValues serializePayload(Message<?> message) {
		Object originalPayload = message.getPayload();
		Object originalContentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
		MessageValues messageValues = new MessageValues(message);
		messageValues.setPayload(originalPayload);
		messageValues.put(MessageHeaders.CONTENT_TYPE, originalContentType);
		return messageValues;
	}



	/**
	 * De-serialize the message payload if necessary.
	 *
	 * @param messageValues message with the payload to deserialize
	 * @param contentTypeResolver used for resolving the mime type.
	 * @return Deserialized Message.
	 */
	public static MessageValues deserializePayload(MessageValues messageValues, ContentTypeResolver contentTypeResolver) {
		Object payload = messageValues.getPayload();
		MimeType contentType = contentTypeResolver.resolve(new MessageHeaders(messageValues.getHeaders()));
		if (payload != null) {
			messageValues.setPayload(payload);
			messageValues.put(MessageHeaders.CONTENT_TYPE, contentType);
		}
		return messageValues;
	}



}
