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

import java.nio.charset.StandardCharsets;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Utility class for serializing and de-serializing the message payload.
 *
 * @author Soby Chacko
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
@Deprecated
public abstract class MessageSerializationUtils {

	/**
	 * Serialize the message payload unless it is a byte array.
	 *
	 * @param message the message with the payload to serialize
	 * @return the Message with the serialized payload
	 */
	public static MessageValues serializePayload(Message<?> message) {
		Object originalPayload = message.getPayload();
		boolean setOriginalContentType = (originalPayload instanceof String);
		Assert.isTrue(originalPayload instanceof byte[] || originalPayload instanceof String,
				"Failed to convert message's payload. No suitable converter found for provided contentType: "
						+ message.getHeaders().get(MessageHeaders.CONTENT_TYPE) + " and paylod: " + originalPayload);
		Object originalContentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
		// Pass content type as String since some transport adapters will exclude
		// CONTENT_TYPE Header otherwise
		String contentType = setOriginalContentType ? JavaClassMimeTypeUtils.mimeTypeFromObject(originalPayload,
				ObjectUtils.nullSafeToString(originalContentType)).toString() : originalContentType.toString() ;

		Object payload = originalPayload instanceof byte[] ? originalPayload : ((String) originalPayload).getBytes(StandardCharsets.UTF_8);
		MessageValues messageValues = new MessageValues(message);
		messageValues.setPayload(payload);
		messageValues.put(MessageHeaders.CONTENT_TYPE, contentType);
		if (originalContentType != null && !originalContentType.toString().equals(contentType.toString())) {
			messageValues.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, originalContentType.toString());
		}
		return messageValues;
	}
}
