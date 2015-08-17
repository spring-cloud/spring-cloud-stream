/*
 * Copyright 2015 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.SelfDescribing;

import org.springframework.cloud.stream.test.binder.TestSupportBinder;
import org.springframework.integration.util.Function;
import org.springframework.messaging.Message;

/**
 * A Hamcrest Matcher meant to be used in conjunction with {@link TestSupportBinder}.
 *
 * <p>Expected usage is of the form (with appropriate static imports):
 * <pre>
 * public class TransformProcessorApplicationTests {
 *
 *    {@literal @}Autowired
 *    {@literal @}ModuleChannels(TransformProcessor.class)
 *    private Processor processor;
 *
 *    {@literal @}Autowired
 *    private MessageCollectorImpl messageCollector;
 *
 *
 *    {@literal @}Test
 *    public void testUsingExpression() {
 *        processor.input().send(new GenericMessage{@literal <}Object>("hello"));
 *        assertThat(messageCollector.forChannel(processor.output()), receivesPayloadThat(is("hellofoo")).within(10));
 *    }
 *
 * }</pre>
 * </p>
 *
 * @author Eric Bottard
 */
public class MessageQueueMatcher<T> extends BaseMatcher<BlockingQueue<Message<?>>> {

	private final Matcher<T> delegate;

	private final long timeout;

	private Extractor<Message<?>, T> extractor;

	private Map<BlockingQueue<Message<?>>, T> actuallyReceived = new HashMap<>();

	private final TimeUnit unit;

	public MessageQueueMatcher(Matcher<T> delegate, long timeout, TimeUnit unit, Extractor<Message<?>, T> extractor) {
		this.delegate = delegate;
		this.timeout = timeout;
		this.unit = unit;
		this.extractor = extractor;
	}


	@Override
	public boolean matches(Object item) {
		@SuppressWarnings("unchecked")
		BlockingQueue<Message<?>> queue = (BlockingQueue<Message<?>>) item;
		Message<?> received = null;
		try {
			if (timeout > 0) {
				received = queue.poll(timeout, unit);
			} else if (timeout == 0) {
				received = queue.poll();
			} else {
				received = queue.take();
			}
		}
		catch (InterruptedException e) {
			return false;
		}
		T unwrapped = extractor.apply(received);
		actuallyReceived.put(queue, unwrapped);
		return delegate.matches(unwrapped);
	}

	@Override
	public void describeMismatch(Object item, Description description) {
		@SuppressWarnings("unchecked")
		BlockingQueue<Message<?>> queue = (BlockingQueue<Message<?>>) item;
		T value = actuallyReceived.get(queue);
		if (value != null) {
			description.appendText("received: ").appendValue(value);
		} else {
			description.appendText("timed out after " + timeout + " " + unit.name().toLowerCase());
		}
	}

	public MessageQueueMatcher<T> within(long timeout, TimeUnit unit) {
		return new MessageQueueMatcher<>(this.delegate, timeout, unit, this.extractor);
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("Channel to receive ").appendDescriptionOf(extractor).appendDescriptionOf(delegate);
	}

	@SuppressWarnings("unchecked")
	public static <P> MessageQueueMatcher<P> receivesMessageThat(Matcher<Message<P>> messageMatcher) {
		return new MessageQueueMatcher(messageMatcher, -1, null, new Extractor<Message<P>, Message<P>>("a message that ") {
			@Override
			public Message<P> apply(Message<P> m) {
				return m;
			}
		});
	}

	@SuppressWarnings("unchecked")
	public static <P> MessageQueueMatcher<P> receivesPayloadThat(Matcher<P> payloadMatcher) {
		return new MessageQueueMatcher(payloadMatcher, -1, null, new Extractor<Message<P>, P>("a message whose payload ") {
			@Override
			public P apply(Message<P> m) {
				return m.getPayload();
			}
		});
	}

	/**
	 * A transformation to be applied to a received message before asserting, <i>e.g.</i> to only inspect the payload.
	 */
	public static abstract class Extractor<R, T> implements Function<R, T>, SelfDescribing {

		private final String behaviorDescription;

		protected Extractor(String behaviorDescription) {
			this.behaviorDescription = behaviorDescription;
		}

		@Override
		public void describeTo(Description description) {
			description.appendText(behaviorDescription);
		}
	}



}
