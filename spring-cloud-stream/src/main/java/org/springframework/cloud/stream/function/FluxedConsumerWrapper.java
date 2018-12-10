/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.util.function.Consumer;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.function.core.FluxWrapper;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @since 2.1
 *
 * Will most likely be moved to SCF
 */
class FluxedConsumerWrapper<T> implements Function<Flux<T>, Mono<Void>>, FluxWrapper<Consumer<Flux<T>>> {

	private final Consumer<Flux<T>> consumer;

	FluxedConsumerWrapper(Consumer<Flux<T>> consumer) {
		this.consumer = consumer;
	}

	@Override
	public Consumer<Flux<T>> getTarget() {
		return consumer;
	}

	@Override
	public Mono<Void> apply(Flux<T> t) {
		return  Mono.fromRunnable(() -> this.consumer.accept(t));
	}

}
