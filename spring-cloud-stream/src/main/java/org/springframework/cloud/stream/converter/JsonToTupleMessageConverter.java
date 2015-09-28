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

import java.util.ArrayList;
import java.util.List;

import org.springframework.messaging.Message;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.cloud.stream.tuple.Tuple;
import org.springframework.cloud.stream.tuple.TupleBuilder;


/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * to convert from a JSON (byte[] or String) to a {@link Tuple}.
 *
 * @author David Turanski
 */
public class JsonToTupleMessageConverter extends AbstractFromMessageConverter {

	private final static List<MimeType> targetMimeTypes = new ArrayList<MimeType>();

	static {
		targetMimeTypes.add(MessageConverterUtils.X_SPRING_TUPLE);
		targetMimeTypes.add(MessageConverterUtils.X_JAVA_OBJECT);
	}

	public JsonToTupleMessageConverter() {
		super(MimeTypeUtils.APPLICATION_JSON, targetMimeTypes);
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return new Class<?>[] { Tuple.class };
	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return new Class<?>[] { byte[].class, String.class };
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass) {
		String source = null;
		if (message.getPayload() instanceof byte[]) {
			source = new String((byte[]) message.getPayload());
		}
		else {
			source = (String) message.getPayload();
		}
		Tuple t = TupleBuilder.fromString(source);
		return buildConvertedMessage(t, message.getHeaders(), MessageConverterUtils.javaObjectMimeType(t.getClass()));
	}
}
