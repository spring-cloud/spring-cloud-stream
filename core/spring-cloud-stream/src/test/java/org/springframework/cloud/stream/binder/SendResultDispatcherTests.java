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

import org.junit.jupiter.api.Test;

import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SendResultDispatcher}.
 *
 * @author Oleg Zhurakousky
 */
class SendResultDispatcherTests {

	private final Message<String> message = new GenericMessage<>("hello");

	@Test
	void notifiesAllListenersInOrder() {
		List<String> calls = new ArrayList<>();
		SendResultDispatcher dispatcher = new SendResultDispatcher("out", List.of(
				recording("first", calls), recording("second", calls)));

		dispatcher.success(this.message, "metadata");
		dispatcher.failure(this.message, new IllegalStateException("boom"));

		assertThat(calls).containsExactly("first:success:out:metadata", "second:success:out:metadata",
				"first:error:out:boom", "second:error:out:boom");
	}

	@Test
	void failingListenerDoesNotPreventOthersFromBeingNotified() {
		List<String> calls = new ArrayList<>();
		SendResultListener failing = new SendResultListener() {

			@Override
			public void onSuccess(String bindingName, Message<?> message, Object result) {
				throw new IllegalStateException("listener failure");
			}

			@Override
			public void onError(String bindingName, Message<?> message, Throwable exception) {
				throw new IllegalStateException("listener failure");
			}

		};
		SendResultDispatcher dispatcher = new SendResultDispatcher("out", List.of(failing, recording("after", calls)));

		dispatcher.success(this.message, null);
		dispatcher.failure(this.message, new IllegalStateException("boom"));

		assertThat(calls).containsExactly("after:success:out:null", "after:error:out:boom");
	}

	private static SendResultListener recording(String name, List<String> calls) {
		return new SendResultListener() {

			@Override
			public void onSuccess(String bindingName, Message<?> message, Object result) {
				calls.add(name + ":success:" + bindingName + ":" + result);
			}

			@Override
			public void onError(String bindingName, Message<?> message, Throwable exception) {
				calls.add(name + ":error:" + bindingName + ":" + exception.getMessage());
			}

		};
	}

}
