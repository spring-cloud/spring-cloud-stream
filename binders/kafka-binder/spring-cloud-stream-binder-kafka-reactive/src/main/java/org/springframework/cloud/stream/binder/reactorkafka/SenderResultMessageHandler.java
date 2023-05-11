/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.reactorkafka;

import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;

import org.springframework.integration.handler.AbstractReactiveMessageHandler;
import org.springframework.messaging.Message;

/**
 * Message handler for {@link SenderResult}s.
 *
 * @param <T> the correlation metadata type.
 *
 * @author Gary Russell
 * @since 4.0.3
 *
 */
public abstract class SenderResultMessageHandler<T> extends AbstractReactiveMessageHandler {

	@SuppressWarnings("unchecked")
	@Override
	protected Mono<Void> handleMessageInternal(Message<?> message) {
		handleResult((SenderResult<T>) message.getPayload());
		return Mono.empty();
	}

	/**
	 * Handle the sender result.
	 *
	 * @param result the result.
	 */
	public abstract void handleResult(SenderResult<T> result);

}
