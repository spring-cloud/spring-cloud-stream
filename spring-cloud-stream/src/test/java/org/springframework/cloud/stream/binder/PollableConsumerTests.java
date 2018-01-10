/*
 * Copyright 2018 the original author or authors.
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

import java.util.Collections;

import org.junit.Test;

import org.springframework.cloud.stream.binder.integration.SpringIntegrationChannelBinder;
import org.springframework.cloud.stream.binder.integration.SpringIntegrationProvisioner;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class PollableConsumerTests {

	@Test
	public void testSimple() {
		SpringIntegrationProvisioner provisioningProvider = new SpringIntegrationProvisioner();
		SpringIntegrationChannelBinder binder = new SpringIntegrationChannelBinder(provisioningProvider);
		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource();
		pollableSource.addInterceptor(new ChannelInterceptorAdapter() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder.withPayload(((String) message.getPayload()).toUpperCase())
						.copyHeaders(message.getHeaders())
						.build();
			}

		});
		binder.bindPollableConsumer("foo", "bar", pollableSource, new ExtendedConsumerProperties<>(null));
		assertThat(pollableSource.poll(received -> {
			assertThat(received.getPayload()).isEqualTo("FOO");
			assertThat(received.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo("text/plain");
		})).isTrue();
	}

	@Test
	public void testEmbedded() {
		SpringIntegrationProvisioner provisioningProvider = new SpringIntegrationProvisioner();
		SpringIntegrationChannelBinder binder = new SpringIntegrationChannelBinder(provisioningProvider);
		binder.setMessageSourceDelegate(() -> {
			MessageValues original = new MessageValues("foo".getBytes(),
					Collections.singletonMap(MessageHeaders.CONTENT_TYPE, "application/octet-stream"));
			byte[] payload = new byte[0];
			try {
				payload = EmbeddedHeaderUtils.embedHeaders(original, MessageHeaders.CONTENT_TYPE);
			}
			catch (Exception e) {
				fail(e.getMessage());
			}
			return new GenericMessage<>(payload);
		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(null);
		properties.setHeaderMode(HeaderMode.embeddedHeaders);
		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource();
		pollableSource.addInterceptor(new ChannelInterceptorAdapter() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder.withPayload(new String((byte[]) message.getPayload()).toUpperCase())
						.copyHeaders(message.getHeaders())
						.build();
			}

		});
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		assertThat(pollableSource.poll(received -> {
			assertThat(received.getPayload()).isEqualTo("FOO");
			assertThat(received.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo("application/octet-stream");
		})).isTrue();
	}

}
