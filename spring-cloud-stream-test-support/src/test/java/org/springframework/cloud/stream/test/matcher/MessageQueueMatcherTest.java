/*
 * Copyright 2015-2019 the original author or authors.
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

package org.springframework.cloud.stream.test.matcher;

import java.util.Collections;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.hamcrest.StringDescription;
import org.junit.Test;

import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests for MessageQueueMatcher.
 *
 * @author Eric Bottard
 */
public class MessageQueueMatcherTest {

	private final BlockingDeque<Message<?>> queue = new LinkedBlockingDeque<>();

	private final StringDescription description = new StringDescription();

	@Test
	public void testTimeout() {
		Message<?> msg = new GenericMessage<>("hello");
		MessageQueueMatcher<?> matcher = MessageQueueMatcher.receivesMessageThat(is(msg))
				.within(2, TimeUnit.MILLISECONDS);

		boolean result = matcher.matches(this.queue);
		assertThat(result).isFalse();
		matcher.describeMismatch(this.queue, this.description);
		assertThat(this.description.toString())
				.isEqualTo("timed out after 2 milliseconds");
	}

	@Test
	public void testMatch() {
		Message<?> msg = new GenericMessage<>("hello");
		MessageQueueMatcher<?> matcher = MessageQueueMatcher.receivesMessageThat(is(msg));
		this.queue.offer(msg);
		boolean result = matcher.matches(this.queue);
		assertThat(result).isTrue();
	}

	@Test
	public void testMismatch() {
		Message<?> msg = new GenericMessage<>("hello");
		Message<?> other = new GenericMessage<>("world");
		MessageQueueMatcher<?> matcher = MessageQueueMatcher.receivesMessageThat(is(msg));
		this.queue.offer(other);
		boolean result = matcher.matches(this.queue);
		assertThat(result).isFalse();
		matcher.describeMismatch(this.queue, this.description);
		assertThat(this.description.toString()).isEqualTo(("received: <" + other + ">"));
	}

	@Test
	public void testExtractor() {
		Message<?> msg = new GenericMessage<>("hello",
				Collections.singletonMap("foo", (Object) "bar"));

		MessageQueueMatcher.Extractor<Message<?>, String> headerExtractor;
		headerExtractor = new MessageQueueMatcher.Extractor<Message<?>, String>(
				"whose 'foo' header") {
			@Override
			public String apply(Message<?> message) {
				return message.getHeaders().get("foo", String.class);
			}
		};
		MessageQueueMatcher<?> matcher = new MessageQueueMatcher<>(is("bar"), -1, null,
				headerExtractor);
		this.queue.offer(msg);
		boolean result = matcher.matches(this.queue);
		assertThat(result);
		matcher = new MessageQueueMatcher<>(is("wizz"), -1, null, headerExtractor);
		this.queue.offer(msg);
		result = matcher.matches(this.queue);
		assertThat(result).isFalse();
		matcher.describeMismatch(this.queue, this.description);
		assertThat(this.description.toString()).isEqualTo(("received: \"bar\""));
	}

	@Test
	public void testDescription() {
		Message<?> msg = new GenericMessage<>("hello");
		MessageQueueMatcher<?> matcher = MessageQueueMatcher.receivesMessageThat(is(msg));
		this.description.appendDescriptionOf(matcher);
		assertThat(this.description.toString())
				.isEqualTo(("Channel to receive a message that is <" + msg + ">"));
	}

}
