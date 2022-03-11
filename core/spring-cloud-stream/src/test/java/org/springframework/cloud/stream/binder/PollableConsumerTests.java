/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.TestChannelBinder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.acks.AcknowledgmentCallback.Status;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @since 2.0
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PollableConsumerTests {

	private ApplicationContext context;

	private SmartMessageConverter messageConverter;

	@BeforeAll
	public void before() {
		this.messageConverter = new CompositeMessageConverterFactory()
				.getMessageConverterForAllRegistered();
	}

	@Test
	void testDefaultMessageSource() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		AtomicInteger count = new AtomicInteger();
		assertThat(pollableSource.poll(message -> {
			assertThat(message).isNotNull();
			count.incrementAndGet();
		})).isTrue();
		assertThat(count.get()).isOne();
	}

	@Test
	void testSimple() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		pollableSource.addInterceptor(new ChannelInterceptor() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder
						.withPayload(((String) message.getPayload()).toUpperCase())
						.copyHeaders(message.getHeaders()).build();
			}

		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final AtomicInteger count = new AtomicInteger();
		assertThat(pollableSource.poll(received -> {
			assertThat(received.getPayload()).isEqualTo("POLLED DATA");
			assertThat(received.getHeaders().get(MessageHeaders.CONTENT_TYPE))
					.isEqualTo(MimeType.valueOf("text/plain"));
			if (count.incrementAndGet() == 1) {
				throw new RuntimeException("test retry");
			}
		})).isTrue();
		assertThat(count.get()).isEqualTo(2);
	}

	@Test
	void testConvertSimple() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		binder.setMessageSourceDelegate(
				() -> new GenericMessage<>("{\"foo\":\"bar\"}".getBytes()));
		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");

		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setMaxAttempts(1);
		properties.setBackOffInitialInterval(0);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final AtomicReference<Object> payload = new AtomicReference<>();
		assertThat(pollableSource.poll(received -> {
			payload.set(received.getPayload());
		}, new ParameterizedTypeReference<Foo>() {
		})).isTrue();
		assertThat(payload.get()).isInstanceOf(Foo.class);
		assertThat(((Foo) payload.get()).getFoo()).isEqualTo("bar");
		// test the cache for coverage
		assertThat(pollableSource.poll(received -> {
			payload.set(received.getPayload());
		}, new ParameterizedTypeReference<Foo>() {
		})).isTrue();
		assertThat(payload.get()).isInstanceOf(Foo.class);
		assertThat(((Foo) payload.get()).getFoo()).isEqualTo("bar");
	}

	@Test
	void testConvertSimpler() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);
		BindingServiceProperties bsps = this.context
				.getBean(BindingServiceProperties.class);
		BindingProperties props = new BindingProperties();
		props.setContentType("text/plain");
		bsps.setBindings(Collections.singletonMap("foo", props));

		binder.setMessageSourceDelegate(() -> new GenericMessage<>("foo".getBytes()));
		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");

		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setMaxAttempts(1);
		properties.setBackOffInitialInterval(0);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final AtomicReference<Object> payload = new AtomicReference<>();
		assertThat(pollableSource.poll(received -> {
			payload.set(received.getPayload());
		}, new ParameterizedTypeReference<String>() {
		})).isTrue();
		assertThat(payload.get()).isInstanceOf(String.class);
		assertThat(payload.get()).isEqualTo("foo");
		// test the cache for coverage
		assertThat(pollableSource.poll(received -> {
			payload.set(received.getPayload());
		}, new ParameterizedTypeReference<String>() {
		})).isTrue();
		assertThat(payload.get()).isInstanceOf(String.class);
		assertThat(payload.get()).isEqualTo("foo");
	}

	@Test
	void testConvertList() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		binder.setMessageSourceDelegate(() -> new GenericMessage<>(
				"[{\"foo\":\"bar\"},{\"foo\":\"baz\"}]".getBytes()));
		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");

		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setMaxAttempts(1);
		properties.setBackOffInitialInterval(0);

		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);

		final AtomicReference<Object> payload = new AtomicReference<>();
		assertThat(pollableSource.poll(received -> {
			payload.set(received.getPayload());
		}, new ParameterizedTypeReference<List<Foo>>() {
		})).isTrue();
		@SuppressWarnings("unchecked")
		List<Foo> list = (List<Foo>) payload.get();
		assertThat(list.size()).isEqualTo(2);
		assertThat(list.get(0).getFoo()).isEqualTo("bar");
		assertThat(list.get(1).getFoo()).isEqualTo("baz");
	}

	@Test
	void testConvertMap() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		binder.setMessageSourceDelegate(
				() -> new GenericMessage<>("{\"qux\":{\"foo\":\"bar\"}}".getBytes()));
		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");

		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setMaxAttempts(1);
		properties.setBackOffInitialInterval(0);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final AtomicReference<Object> payload = new AtomicReference<>();
		assertThat(pollableSource.poll(received -> {
			payload.set(received.getPayload());
		}, new ParameterizedTypeReference<Map<String, Foo>>() {
		})).isTrue();
		@SuppressWarnings("unchecked")
		Map<String, Foo> map = (Map<String, Foo>) payload.get();
		assertThat(map.size()).isEqualTo(1);
		assertThat(map.get("qux").getFoo()).isEqualTo("bar");
	}

	@Test
	void testEmbedded() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		binder.setMessageSourceDelegate(() -> {
			MessageValues original = new MessageValues("foo".getBytes(),
					Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
							"application/octet-stream"));
			byte[] payload = new byte[0];
			try {
				payload = EmbeddedHeaderUtils.embedHeaders(original,
						MessageHeaders.CONTENT_TYPE);
			}
			catch (Exception e) {
				fail(e.getMessage());
			}
			return new GenericMessage<>(payload);
		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setHeaderMode(HeaderMode.embeddedHeaders);
		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		pollableSource.addInterceptor(new ChannelInterceptor() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder
						.withPayload(
								new String((byte[]) message.getPayload()).toUpperCase())
						.copyHeaders(message.getHeaders()).build();
			}

		});
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		assertThat(pollableSource.poll(received -> {
			assertThat(received.getPayload()).isEqualTo("FOO");
			assertThat(received.getHeaders().get(MessageHeaders.CONTENT_TYPE))
					.isEqualTo("application/octet-stream");
		})).isTrue();
	}

	@Test
	void testErrors() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		pollableSource.addInterceptor(new ChannelInterceptor() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder
						.withPayload(((String) message.getPayload()).toUpperCase())
						.copyHeaders(message.getHeaders()).build();
			}

		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		properties.getRetryableExceptions().put(IllegalStateException.class, false);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final CountDownLatch latch = new CountDownLatch(1);
		this.context.getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
				SubscribableChannel.class).subscribe(m -> {
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
		assertThat(((Exception) lastError.getPayload()).getCause().getMessage())
				.isEqualTo("test recoverer");
		assertThat(pollableSource.poll(received -> {
			count.incrementAndGet();
			throw new IllegalStateException("no retries");
		})).isTrue();
		assertThat(count.get()).isEqualTo(3);
		lastError = binder.getLastError();
		assertThat(lastError).isNotNull();
		assertThat(((Exception) lastError.getPayload()).getCause().getMessage())
				.isEqualTo("no retries");
	}

	@Test
	void testErrorsNoRetry() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		pollableSource.addInterceptor(new ChannelInterceptor() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder
						.withPayload(((String) message.getPayload()).toUpperCase())
						.copyHeaders(message.getHeaders()).build();
			}

		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setMaxAttempts(1);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final CountDownLatch latch = new CountDownLatch(1);
		this.context.getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME,
				SubscribableChannel.class).subscribe(m -> {
					latch.countDown();
				});
		final AtomicInteger count = new AtomicInteger();
		assertThat(pollableSource.poll(received -> {
			count.incrementAndGet();
			throw new RuntimeException("test recoverer");
		})).isTrue();
		assertThat(count.get()).isEqualTo(1);
	}

	@Test
	void testRequeue() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		AcknowledgmentCallback callback = mock(AcknowledgmentCallback.class);
		pollableSource.addInterceptor(new ChannelInterceptor() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder.fromMessage(message)
						.setHeader(
								IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK,
								callback)
						.build();
			}

		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(null);
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final AtomicInteger count = new AtomicInteger();
		try {
			assertThat(pollableSource.poll(received -> {
				count.incrementAndGet();
				throw new RequeueCurrentMessageException("test retry");
			})).isTrue();
		}
		catch (Exception e) {
			// no op
		}
		assertThat(count.get()).isEqualTo(2);
		verify(callback).acknowledge(Status.REQUEUE);
	}

	@Test
	void testRequeueWithNoAcknowledgementCallback() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		pollableSource.addInterceptor(new ChannelInterceptor() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder.fromMessage(message)
						.build();
			}

		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(null);
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		final AtomicInteger count = new AtomicInteger();

		assertThat(pollableSource.poll(received -> {
			count.incrementAndGet();
			throw new RequeueCurrentMessageException("test retry");
		})).isTrue();

		assertThat(count.get()).isEqualTo(2);

	}

	@Test
	void testRequeueFromErrorFlow() {
		TestChannelBinder binder = createBinder();
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		AcknowledgmentCallback callback = mock(AcknowledgmentCallback.class);
		pollableSource.addInterceptor(new ChannelInterceptor() {

			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				return MessageBuilder.fromMessage(message)
						.setHeader(
								IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK,
								callback)
						.build();
			}

		});
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(null);
		properties.setMaxAttempts(1);
		binder.bindPollableConsumer("foo", "bar", pollableSource, properties);
		SubscribableChannel errorChannel = new DirectChannel();
		errorChannel.subscribe(msg -> {
			throw new RequeueCurrentMessageException((Throwable) msg.getPayload());
		});
		pollableSource.setErrorChannel(errorChannel);
		try {
			pollableSource.poll(received -> {
				throw new RuntimeException("test requeue from error flow");
			});
		}
		catch (Exception e) {
			// no op
		}
		verify(callback).acknowledge(Status.REQUEUE);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testAutoStartupOff() {
		TestChannelBinder binder = createBinder();
		binder.setMessageSourceDelegate(new LifecycleMessageSource(
				() -> new GenericMessage<>("{\"foo\":\"bar\"}".getBytes())));
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setAutoStartup(false);

		Binding<PollableSource<MessageHandler>> pollableSourceBinding = binder
				.bindPollableConsumer("foo", "bar", pollableSource, properties);

		assertThat(pollableSourceBinding.isRunning()).isFalse();
	}

	@SuppressWarnings("unchecked")
	@Test
	void testAutoStartupOn() {
		TestChannelBinder binder = createBinder();
		binder.setMessageSourceDelegate(new LifecycleMessageSource(
				() -> new GenericMessage<>("{\"foo\":\"bar\"}".getBytes())));
		MessageConverterConfigurer configurer = this.context
				.getBean(MessageConverterConfigurer.class);

		DefaultPollableMessageSource pollableSource = new DefaultPollableMessageSource(
				this.messageConverter);
		configurer.configurePolledMessageSource(pollableSource, "foo");
		ExtendedConsumerProperties<Object> properties = new ExtendedConsumerProperties<>(
				null);
		properties.setAutoStartup(true);

		Binding<PollableSource<MessageHandler>> pollableSourceBinding = binder
				.bindPollableConsumer("foo", "bar", pollableSource, properties);

		assertThat(pollableSourceBinding.isRunning()).isTrue();
	}

	private TestChannelBinder createBinder(String... args) {
		this.context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration())
						.web(WebApplicationType.NONE).run(args);
		TestChannelBinder binder = this.context.getBean(TestChannelBinder.class);
		return binder;
	}

	public static class Foo {

		private String foo;

		protected String getFoo() {
			return this.foo;
		}

		protected void setFoo(String foo) {
			this.foo = foo;
		}

	}

	public static class LifecycleMessageSource<T> implements MessageSource<T>, Lifecycle {
		private final MessageSource<T> delegate;

		private boolean running = false;

		public LifecycleMessageSource(MessageSource<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Message<T> receive() {
			return this.delegate.receive();
		}

		@Override
		public void start() {
			this.running = true;
		}

		@Override
		public void stop() {
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}
	}

}
