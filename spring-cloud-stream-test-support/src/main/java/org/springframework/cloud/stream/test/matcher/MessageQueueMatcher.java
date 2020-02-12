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

package org.springframework.cloud.stream.test.matcher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.SelfDescribing;

import org.springframework.cloud.stream.test.binder.TestSupportBinder;
import org.springframework.messaging.Message;

/**
 * A Hamcrest Matcher meant to be used in conjunction with {@link TestSupportBinder}.
 *
 *
 * @param <T> return type
 * @author Eric Bottard
 * @author Janne Valkealahti
 */
public class MessageQueueMatcher<T> extends BaseMatcher<BlockingQueue<Message<?>>> {

	private final Matcher<T> delegate;

	private final long timeout;

	private final TimeUnit unit;

	private Extractor<Message<?>, T> extractor;

	private Map<BlockingQueue<Message<?>>, T> actuallyReceived = new HashMap<>();

	public MessageQueueMatcher(Matcher<T> delegate, long timeout, TimeUnit unit,
			Extractor<Message<?>, T> extractor) {
		this.delegate = delegate;
		this.timeout = timeout;
		this.unit = (unit != null ? unit : TimeUnit.SECONDS);
		this.extractor = extractor;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <P> MessageQueueMatcher<P> receivesMessageThat(
			Matcher<Message<P>> messageMatcher) {
		return new MessageQueueMatcher(messageMatcher, 5, TimeUnit.SECONDS,
				new Extractor<Message<P>, Message<P>>("a message that ") {
					@Override
					public Message<P> apply(Message<P> m) {
						return m;
					}
				});
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <P> MessageQueueMatcher<P> receivesPayloadThat(
			Matcher<P> payloadMatcher) {
		return new MessageQueueMatcher(payloadMatcher, 5, TimeUnit.SECONDS,
				new Extractor<Message<P>, P>("a message whose payload ") {
					@Override
					public P apply(Message<P> m) {
						return m.getPayload();
					}
				});
	}

	@Override
	public boolean matches(Object item) {
		@SuppressWarnings("unchecked")
		BlockingQueue<Message<?>> queue = (BlockingQueue<Message<?>>) item;
		Message<?> received = null;
		try {
			if (this.timeout > 0) {
				received = queue.poll(this.timeout, this.unit);
			}
			else if (this.timeout == 0) {
				received = queue.poll();
			}
			else {
				received = queue.take();
			}
		}
		catch (InterruptedException e) {
			return false;
		}
		T unwrapped = this.extractor.apply(received);
		this.actuallyReceived.put(queue, unwrapped);
		return this.delegate.matches(unwrapped);
	}

	@Override
	public void describeMismatch(Object item, Description description) {
		@SuppressWarnings("unchecked")
		BlockingQueue<Message<?>> queue = (BlockingQueue<Message<?>>) item;
		T value = this.actuallyReceived.get(queue);
		if (value != null) {
			description.appendText("received: ").appendValue(value);
		}
		else {
			description.appendText("timed out after " + this.timeout + " "
					+ this.unit.name().toLowerCase());
		}
	}

	public MessageQueueMatcher<T> within(long timeout, TimeUnit unit) {
		return new MessageQueueMatcher<>(this.delegate, timeout, unit, this.extractor);
	}

	public MessageQueueMatcher<T> immediately() {
		return new MessageQueueMatcher<>(this.delegate, 0, null, this.extractor);
	}

	public MessageQueueMatcher<T> indefinitely() {
		return new MessageQueueMatcher<>(this.delegate, -1, null, this.extractor);
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("Channel to receive ").appendDescriptionOf(this.extractor)
				.appendDescriptionOf(this.delegate);
	}

	/**
	 * A transformation to be applied to a received message before asserting, <i>e.g.</i>
	 * to only inspect the payload.
	 *
	 * @param <R> input type
	 * @param <T> return type
	 */
	public static abstract class Extractor<R, T>
			implements Function<R, T>, SelfDescribing {

		private final String behaviorDescription;

		protected Extractor(String behaviorDescription) {
			this.behaviorDescription = behaviorDescription;
		}

		@Override
		public void describeTo(Description description) {
			description.appendText(this.behaviorDescription);
		}

	}

}
