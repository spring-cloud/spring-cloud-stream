/*
 * Copyright 2023-2023 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.messaging.MessageHeaders;
import org.springframework.pulsar.support.header.PulsarHeaderMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PulsarBinderHeaderMapper}.
 *
 * @author Chris Bono
 */
@ExtendWith(MockitoExtension.class)
class PulsarBinderHeaderMapperTests {

	@Mock
	private PulsarHeaderMapper delegateMapper;

	@InjectMocks
	private PulsarBinderHeaderMapper binderHeaderMapper;

	@Nested
	class ToPulsarHeadersOutboundTests {

		@Test
		void delegateReturnsEmptyHeaders() {
			var delegatePulsarHeaders = new HashMap<String, String>();
			when(delegateMapper.toPulsarHeaders(any(MessageHeaders.class))).thenReturn(delegatePulsarHeaders);
			var springHeaders = mock(MessageHeaders.class);
			var pulsarHeaders = binderHeaderMapper.toPulsarHeaders(springHeaders);
			verify(delegateMapper).toPulsarHeaders(springHeaders);
			assertThat(pulsarHeaders).isEmpty();
		}

		@Test
		void neverHeadersRemovedFromDelegateHeaders() {
			var delegatePulsarHeaders = new HashMap<String, String>();
			delegatePulsarHeaders.put(MessageHeaders.ID, "5150");
			delegatePulsarHeaders.put(MessageHeaders.TIMESTAMP, "12345");
			delegatePulsarHeaders.put(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT, "5");
			delegatePulsarHeaders.put(BinderHeaders.NATIVE_HEADERS_PRESENT, "true");
			delegatePulsarHeaders.put("foo", "bar");
			when(delegateMapper.toPulsarHeaders(any(MessageHeaders.class))).thenReturn(delegatePulsarHeaders);
			var springHeaders = mock(MessageHeaders.class);
			var pulsarHeaders = binderHeaderMapper.toPulsarHeaders(springHeaders);
			verify(delegateMapper).toPulsarHeaders(springHeaders);
			assertThat(pulsarHeaders).containsOnly(entry("foo", "bar"));
		}

	}

	@Nested
	class ToSpringHeadersInboundTests {

		@Test
		void delegateReturnsEmptyHeaders() {
			var emptyDelegateHeaders = mock(MessageHeaders.class);
			when(emptyDelegateHeaders.isEmpty()).thenReturn(true);
			when(delegateMapper.toSpringHeaders(any(Message.class))).thenReturn(emptyDelegateHeaders);
			var springHeaders = binderHeaderMapper.toSpringHeaders(mock(Message.class));
			assertThat(springHeaders).isSameAs(emptyDelegateHeaders);
			verify(springHeaders).isEmpty();
			verifyNoMoreInteractions(springHeaders);
		}

		@Test
		void nativeHeadersIndicatorAddedToDelegateHeaders() {
			var delegateSpringHeaders = new MessageHeaders(Map.of("foo", "bar"));
			when(delegateMapper.toSpringHeaders(any(Message.class))).thenReturn(delegateSpringHeaders);
			var pulsarMessage = mock(Message.class);
			var springHeaders = binderHeaderMapper.toSpringHeaders(pulsarMessage);
			verify(delegateMapper).toSpringHeaders(pulsarMessage);
			assertThat(springHeaders).containsEntry("foo", "bar").containsEntry(BinderHeaders.NATIVE_HEADERS_PRESENT,
					Boolean.TRUE);

		}

	}

}
