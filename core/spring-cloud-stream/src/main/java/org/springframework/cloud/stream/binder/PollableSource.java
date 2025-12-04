/*
 * Copyright 2018-present the original author or authors.
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

import org.springframework.core.ParameterizedTypeReference;

/**
 * A mechanism to poll a consumer.
 *
 * @param <H> the handler type to process the result of the poll.
 * @author Gary Russell
 * @since 2.0
 *
 */
@FunctionalInterface
public interface PollableSource<H> {

	/**
	 * Poll the consumer.
	 * @param handler the handler.
	 * @return true if a message was handled.
	 */
	boolean poll(H handler);

	/**
	 * Poll the consumer and convert the payload to the type. Throw a
	 * {@code RequeueCurrentMessageException} to force the current message to be requeued
	 * in the broker (after retries are exhausted, if configured).
	 * @param handler the handler.
	 * @param type the type.
	 * @return true if a message was handled.
	 */
	default boolean poll(H handler, ParameterizedTypeReference<?> type) {
		return poll(handler);
	}

}
