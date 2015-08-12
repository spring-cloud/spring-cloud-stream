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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.SelfDescribing;

import org.springframework.cloud.stream.test.binder.TestSupportBinder;
import org.springframework.integration.util.Function;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

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
 *    {@literal @}Test
 *    public void testUsingExpression() {
 *    processor.input().send(new GenericMessage{@literal <}Object>("hello"));
 *    assertThat(processor.output(), receivesPayloadThat(equalTo("hellofoo")).within(10));
 *    }
 *
 * }</pre>
 * </p>
 *
 * @author Eric Bottard
 */
public class MessageChannelMatcher<T> extends BaseMatcher<MessageChannel> {

	private final Matcher<T> delegate;

	private final long timeout;

	private Extractor<Message<?>, T> extractor;

	private Map<MessageChannel, T> actuallyReceived = new HashMap<>();

	public MessageChannelMatcher(Matcher<T> delegate, long timeout, Extractor<Message<?>, T> extractor) {
		this.delegate = delegate;
		this.timeout = timeout;
		this.extractor = extractor;
	}


	@Override
	public boolean matches(Object item) {
		MessageChannel channel = (MessageChannel) item;
		Message<?> received = TestSupportBinder.channelToBindings.get(channel).receive(timeout);
		if (received == null) { // timeout
			return false;
		}
		T unwrapped = extractor.apply(received);
		actuallyReceived.put(channel, unwrapped);
		return delegate.matches(unwrapped);
	}

	@Override
	public void describeMismatch(Object item, Description description) {
		T value = actuallyReceived.get(item);
		if (value != null) {
			description.appendText("received: ").appendValue(value);
		} else {
			description.appendText("timed out after " + timeout + "ms.");
		}
	}

	public MessageChannelMatcher within(long timeout) {
		return new MessageChannelMatcher(this.delegate, timeout, extractor);
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("Channel to receive ").appendDescriptionOf(extractor).appendDescriptionOf(delegate);
	}

	public static <P> MessageChannelMatcher receivesMessageThat(Matcher<Message<P>> messageMatcher) {
		return new MessageChannelMatcher(messageMatcher, -1, new Extractor<Message<P>, Message<P>>("a message that ") {
			@Override
			public Message<P> apply(Message<P> m) {
				return m;
			}
		});
	}

	public static <P> MessageChannelMatcher receivesPayloadThat(Matcher<P> payloadMatcher) {
		return new MessageChannelMatcher(payloadMatcher, -1, new Extractor<Message<P>, P>("a message whose payload ") {
			@Override
			public P apply(Message<P> m) {
				return m.getPayload();
			}
		});
	}

	private static abstract class Extractor<R, T> implements Function<R, T>, SelfDescribing {

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
