/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.reactive;

import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import org.springframework.util.Assert;

/**
 * Default {@link org.springframework.cloud.stream.reactive.FluxSender} implementation.
 * This implementation may be used for cancelling a subscription on the underlying
 * {@link reactor.core.publisher.Flux}.
 *
 * @author Soby Chacko
 * @since 1.3.0
 */
class DefaultFluxSender implements FluxSender {

	private final Consumer<Object> consumer;

	private Log log = LogFactory.getLog(DefaultFluxSender.class);

	private volatile Disposable disposable;

	DefaultFluxSender(Consumer<Object> consumer) {
		Assert.notNull(consumer, "Consumer must not be null");
		this.consumer = consumer;
	}

	@Override
	public Mono<Void> send(Flux<?> flux) {
		MonoProcessor<Void> sendResult = MonoProcessor.create();
		// add error handling and reconnect in the event of an error
		this.disposable = flux
				.doOnError(e -> this.log.error("Error during processing: ", e)).retry()
				.subscribe(this.consumer, sendResult::onError, sendResult::onComplete);
		return sendResult;
	}

	@Override
	public void close() {
		if (this.disposable != null) {
			this.disposable.dispose();
		}
	}

}
