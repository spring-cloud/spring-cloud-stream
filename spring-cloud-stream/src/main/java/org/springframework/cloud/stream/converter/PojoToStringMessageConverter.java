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

import org.springframework.cloud.stream.tuple.Tuple;
import org.springframework.cloud.stream.tuple.TupleBuilder;
import org.springframework.messaging.Message;
import org.springframework.util.MimeTypeUtils;


/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * to convert a Java object to a String using toString()
 *
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 */
public class PojoToStringMessageConverter extends AbstractFromMessageConverter {

	public PojoToStringMessageConverter() {
		super(MimeTypeUtils.TEXT_PLAIN);
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return new Class<?>[] { String.class };
	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return null;
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass, Object hint) {
		String payloadString = null;
		if (message.getPayload() instanceof Tuple) {
			TupleBuilder builder = TupleBuilder.tuple();
			builder.putAll((Tuple)message.getPayload());
			payloadString = builder.build().toString();
		}
		else {
			payloadString = message.getPayload().toString();
		}
		return buildConvertedMessage(payloadString, message.getHeaders(), MimeTypeUtils.TEXT_PLAIN);
	}

}
