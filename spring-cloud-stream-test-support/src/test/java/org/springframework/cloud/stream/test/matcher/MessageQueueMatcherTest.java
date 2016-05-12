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

package org.springframework.cloud.stream.test.matcher;

import java.util.Collections;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.hamcrest.StringDescription;
import org.junit.Test;

import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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
		MessageQueueMatcher<?> matcher = MessageQueueMatcher.receivesMessageThat(is(msg)).within(2, TimeUnit.MILLISECONDS);

		boolean result = matcher.matches(queue);
		assertThat(result, is(false));
		matcher.describeMismatch(queue, description);
		assertThat(description.toString(), is("timed out after 2 milliseconds"));
	}

	@Test
	public void testMatch() {
		Message<?> msg = new GenericMessage<>("hello");
		MessageQueueMatcher<?> matcher = MessageQueueMatcher.receivesMessageThat(is(msg));

		queue.offer(msg);

		boolean result = matcher.matches(queue);
		assertThat(result, is(true));
	}

	@Test
	public void testMisMatch() {
		Message<?> msg = new GenericMessage<>("hello");
		Message<?> other = new GenericMessage<>("world");

		MessageQueueMatcher<?> matcher = MessageQueueMatcher.receivesMessageThat(is(msg));

		queue.offer(other);

		boolean result = matcher.matches(queue);
		assertThat(result, is(false));
		matcher.describeMismatch(queue, description);
		assertThat(description.toString(), is("received: <" + other + ">"));
	}

	@Test
	public void testExtractor() {
		Message<?> msg = new GenericMessage<>("hello", Collections.singletonMap("foo", (Object) "bar"));

		MessageQueueMatcher.Extractor<Message<?>, String> headerExtractor = new MessageQueueMatcher.Extractor<Message<?>, String>("whose 'foo' header") {
			@Override
			public String apply(Message<?> message) {
				return message.getHeaders().get("foo", String.class);
			}
		};

		MessageQueueMatcher<?> matcher = new MessageQueueMatcher<>(is("bar"), -1, null, headerExtractor);
		queue.offer(msg);
		boolean result = matcher.matches(queue);
		assertThat(result, is(true));

		matcher = new MessageQueueMatcher<>(is("wizz"), -1, null, headerExtractor);
		queue.offer(msg);
		result = matcher.matches(queue);
		assertThat(result, is(false));
		matcher.describeMismatch(queue, description);
		assertThat(description.toString(), is("received: \"bar\""));
	}

	@Test
	public void testDescription() {
		Message<?> msg = new GenericMessage<>("hello");

		MessageQueueMatcher<?> matcher = MessageQueueMatcher.receivesMessageThat(is(msg));

		description.appendDescriptionOf(matcher);
		assertThat(description.toString(), is("Channel to receive a message that is <" + msg + ">"));
	}
}
