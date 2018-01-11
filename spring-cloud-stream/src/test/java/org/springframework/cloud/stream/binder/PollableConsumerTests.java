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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.springframework.cloud.stream.binder.integration.SpringIntegrationChannelBinder;
import org.springframework.cloud.stream.binder.integration.SpringIntegrationProvisioner;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
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

	private final GenericApplicationContext context = new GenericApplicationContext();

	@Test
	public void testSimple() {
		SpringIntegrationChannelBinder binder = createBinder();
		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource();
		pollableSource.addInterceptor(new ChannelInterceptorAdapter() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder.withPayload(((String) message.getPayload()).toUpperCase())
						.copyHeaders(message.getHeaders())
						.build();
			}

		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(null);
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final AtomicInteger count = new AtomicInteger();
		assertThat(pollableSource.poll(received -> {
			assertThat(received.getPayload()).isEqualTo("POLLED DATA");
			assertThat(received.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo("text/plain");
			if (count.incrementAndGet() == 1) {
				throw new RuntimeException("test retry");
			}
		})).isTrue();
		assertThat(count.get()).isEqualTo(2);
	}

	@Test
	public void testEmbedded() {
		SpringIntegrationChannelBinder binder = createBinder();
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

	@Test
	public void testErrors() {
		SpringIntegrationChannelBinder binder = createBinder();
		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource();
		pollableSource.addInterceptor(new ChannelInterceptorAdapter() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder.withPayload(((String) message.getPayload()).toUpperCase())
						.copyHeaders(message.getHeaders())
						.build();
			}

		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(null);
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final CountDownLatch latch = new CountDownLatch(1);
		this.context.getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME, SubscribableChannel.class).subscribe(m -> {
			latch.countDown();
		});
		final AtomicInteger count = new AtomicInteger();
		assertThat(pollableSource.poll(received -> {
			count.incrementAndGet();
			throw new RuntimeException("test recoverer");
		})).isTrue();
		assertThat(count.get()).isEqualTo(2);
		Message<?> lastError = binder.getLastError();
		assertThat(lastError).isNotNull();
		assertThat(((Exception) lastError.getPayload()).getCause().getMessage()).isEqualTo("test recoverer");
	}

	private SpringIntegrationChannelBinder createBinder() {
		SpringIntegrationProvisioner provisioningProvider = new SpringIntegrationProvisioner();
		SpringIntegrationChannelBinder binder = new SpringIntegrationChannelBinder(provisioningProvider);
		this.context.registerBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME, PublishSubscribeChannel.class);
		this.context.refresh();
		binder.setApplicationContext(this.context);
		return binder;
	}

}
