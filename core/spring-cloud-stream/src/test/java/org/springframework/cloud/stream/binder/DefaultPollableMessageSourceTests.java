/*
 * Copyright 2026-present the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link DefaultPollableMessageSource} invokes the full
 * {@link ChannelInterceptor} contract around poll lifecycles (GH-3131),
 * while keeping the legacy {@code preSend} behavior intact.
 */
class DefaultPollableMessageSourceTests {

	private final List<String> events = new ArrayList<>();

	private final AtomicInteger receiveCount = new AtomicInteger();

	@Test
	void successfulPollInvokesFullInterceptorLifecycleInOrder() {
		DefaultPollableMessageSource source = newSource();
		source.setSource(this::message);
		source.addInterceptor(recorder());

		assertThat(source.poll(noopHandler())).isTrue();

		assertThat(this.events).containsExactly(
			"preReceive",
			"postReceive",
			"preSend",
			"afterReceiveCompletion",
			"postSend",
			"afterSendCompletion");
	}

	@Test
	void falsePreReceiveShortCircuitsReceive() {
		DefaultPollableMessageSource source = newSource();
		source.setSource(this::countingMessage);
		source.addInterceptor(new Recorder() {
			@Override
			public boolean preReceive(MessageChannel channel) {
				DefaultPollableMessageSourceTests.this.events.add("preReceive");
				return false;
			}
		});

		assertThat(source.poll(noopHandler())).isFalse();
		assertThat(this.events).containsExactly("preReceive");
		assertThat(this.receiveCount.get()).isZero();
	}

	@Test
	void nullPostReceiveAbortsFurtherProcessing() {
		DefaultPollableMessageSource source = newSource();
		source.setSource(this::countingMessage);
		source.addInterceptor(new Recorder() {
			@Override
			public Message<?> postReceive(Message<?> message, MessageChannel channel) {
				DefaultPollableMessageSourceTests.this.events.add("postReceive");
				return null;
			}
		});

		assertThat(source.poll(noopHandler())).isFalse();
		assertThat(this.receiveCount.get()).isEqualTo(1);
		assertThat(this.events).containsExactly(
			"preReceive",
			"postReceive",
			"afterReceiveCompletion");
	}

	@Test
	void handlerFailureSkipsPostSendAndReportsExceptionOnCompletion() {
		DefaultPollableMessageSource source = newSource();
		DirectChannel errorChannel = new DirectChannel();
		errorChannel.subscribe(message -> {
		});
		source.setErrorChannel(errorChannel);
		source.setSource(this::message);
		source.addInterceptor(recorder());
		MessageHandler failingHandler = message -> {
			throw new IllegalStateException("boom");
		};

		assertThat(source.poll(failingHandler)).isTrue();

		assertThat(this.events).containsExactly(
			"preReceive",
			"postReceive",
			"preSend",
			"afterReceiveCompletion",
			"afterSendCompletion:exception");
	}

	@Test
	void interceptorsAddedAfterSetSourceAreHonored() {
		DefaultPollableMessageSource source = newSource();
		source.setSource(this::message);
		source.addInterceptor(recorder());

		assertThat(source.poll(noopHandler())).isTrue();

		assertThat(this.events).contains("postSend", "afterSendCompletion");
	}

	@Test
	void nullPreSendStillAbortsLegacyPath() {
		DefaultPollableMessageSource source = newSource();
		source.setSource(this::countingMessage);
		source.addInterceptor(new Recorder() {
			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				DefaultPollableMessageSourceTests.this.events.add("preSend");
				return null;
			}
		});

		assertThat(source.poll(noopHandler())).isFalse();
		assertThat(this.receiveCount.get()).isEqualTo(1);
		assertThat(this.events).containsExactly(
			"preReceive",
			"postReceive",
			"preSend",
			"afterReceiveCompletion");
	}

	private DefaultPollableMessageSource newSource() {
		return new DefaultPollableMessageSource(null);
	}

	private Message<Object> message() {
		return MessageBuilder.withPayload((Object) "hello").build();
	}

	private Message<Object> countingMessage() {
		this.receiveCount.incrementAndGet();
		return message();
	}

	private ChannelInterceptor recorder() {
		return new Recorder();
	}

	private MessageHandler noopHandler() {
		return message -> {
		};
	}

	private class Recorder implements ChannelInterceptor {

		@Override
		public boolean preReceive(MessageChannel channel) {
			DefaultPollableMessageSourceTests.this.events.add("preReceive");
			return true;
		}

		@Override
		public Message<?> postReceive(Message<?> message, MessageChannel channel) {
			DefaultPollableMessageSourceTests.this.events.add("postReceive");
			return message;
		}

		@Override
		public void afterReceiveCompletion(Message<?> message, MessageChannel channel,
				Exception ex) {
			record("afterReceiveCompletion", ex);
		}

		@Override
		public Message<?> preSend(Message<?> message, MessageChannel channel) {
			DefaultPollableMessageSourceTests.this.events.add("preSend");
			return message;
		}

		@Override
		public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
			DefaultPollableMessageSourceTests.this.events.add("postSend");
		}

		@Override
		public void afterSendCompletion(Message<?> message, MessageChannel channel,
				boolean sent, Exception ex) {
			record("afterSendCompletion", ex);
		}

		private void record(String name, Exception ex) {
			DefaultPollableMessageSourceTests.this.events
				.add(ex != null ? name + ":exception" : name);
		}

	}

}
