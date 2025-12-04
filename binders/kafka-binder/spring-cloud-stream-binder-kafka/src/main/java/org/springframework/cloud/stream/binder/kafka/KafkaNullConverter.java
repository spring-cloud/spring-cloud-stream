/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.util.Collections;

import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeTypeUtils;

/**
 * A {@link MessageConverter} that supports {@link KafkaNull} payloads.
 *
 * @author Gary Russell
 * @author Aldo Sinanaj
 * @since 2.2
 */
public class KafkaNullConverter extends AbstractMessageConverter {

	public KafkaNullConverter() {
		super(Collections.singletonList(MimeTypeUtils.ALL));
	}

	@Override
	protected boolean supportsMimeType(MessageHeaders headers) {
		return true;
	}

	@Override
	protected boolean supports(Class<?> aClass) {
		return KafkaNull.class.equals(aClass);
	}

	@Override
	protected boolean canConvertFrom(Message<?> message, Class<?> targetClass) {
		return message.getPayload() instanceof KafkaNull;
	}

	@Override
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass,
			Object conversionHint) {
		return message.getPayload();
	}

	@Override
	protected Object convertToInternal(Object payload, MessageHeaders headers,
			Object conversionHint) {
		return payload;
	}

}
