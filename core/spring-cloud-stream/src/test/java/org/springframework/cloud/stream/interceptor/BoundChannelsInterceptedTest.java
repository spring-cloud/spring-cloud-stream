/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.interceptor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.config.GlobalChannelInterceptor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.MimeTypeUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Verifies that interceptors used by modules are applied correctly to generated channels.
 *
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 */
@ExtendWith(SpringExtension.class)
// @checkstyle:off
@SpringBootTest(classes = BoundChannelsInterceptedTest.Foo.class, properties = "spring.cloud.stream.default-binder=mock")
public class BoundChannelsInterceptedTest {

	// @checkstyle:on

	public static final Message<?> TEST_MESSAGE = MessageBuilder.withPayload("bar")
			.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
			.build();

	@Autowired
	ChannelInterceptor channelInterceptor;

	@Autowired
	private Sink sink;

	@Test
	public void testBoundChannelsIntercepted() {
		this.sink.input().send(TEST_MESSAGE);
		verify(this.channelInterceptor).preSend(Mockito.any(),
				Mockito.eq(this.sink.input()));
		verifyNoMoreInteractions(this.channelInterceptor);
	}

	@SpringBootApplication
	@EnableBinding(Sink.class)
	public static class Foo {

		@ServiceActivator(inputChannel = Sink.INPUT)
		public void fooSink(Message<?> message) {
		}

		@Bean
		@GlobalChannelInterceptor
		public ChannelInterceptor globalChannelInterceptor() {
			return mock(ChannelInterceptor.class);
		}

	}

}
