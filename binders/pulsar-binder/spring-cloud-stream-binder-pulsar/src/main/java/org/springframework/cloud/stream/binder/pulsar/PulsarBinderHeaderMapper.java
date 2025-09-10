/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar;

import java.util.Map;

import org.apache.pulsar.client.api.Message;

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.pulsar.support.header.PulsarHeaderMapper;

/**
 * A delegating {@code PulsarHeaderMapper} that ensures the delegate mapper never includes
 * internal binder specific headers during outbound mapping.
 *
 * @author Chris Bono
 */
class PulsarBinderHeaderMapper implements PulsarHeaderMapper {

	private final PulsarHeaderMapper delegate;

	/**
	 * Construct a mapper with the specified delegate.
	 * @param delegate the delegate mapper
	 */
	PulsarBinderHeaderMapper(PulsarHeaderMapper delegate) {
		this.delegate = delegate;
	}

	@Override
	public Map<String, String> toPulsarHeaders(MessageHeaders springHeaders) {
		Map<String, String> pulsarHeaders = this.delegate.toPulsarHeaders(springHeaders);
		pulsarHeaders.remove(MessageHeaders.ID);
		pulsarHeaders.remove(MessageHeaders.TIMESTAMP);
		pulsarHeaders.remove(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT);
		pulsarHeaders.remove(BinderHeaders.NATIVE_HEADERS_PRESENT);
		return pulsarHeaders;
	}

	@Override
	public MessageHeaders toSpringHeaders(Message<?> pulsarMessage) {
		var springHeaders = this.delegate.toSpringHeaders(pulsarMessage);
		if (!springHeaders.isEmpty()) {
			MessageHeaderAccessor mutableHeaders = new MessageHeaderAccessor();
			mutableHeaders.copyHeaders(springHeaders);
			mutableHeaders.setHeader(BinderHeaders.NATIVE_HEADERS_PRESENT, Boolean.TRUE);
			springHeaders = mutableHeaders.getMessageHeaders();
		}
		return springHeaders;
	}

}
