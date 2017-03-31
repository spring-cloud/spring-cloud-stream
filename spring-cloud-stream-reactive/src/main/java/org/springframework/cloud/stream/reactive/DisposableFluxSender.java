/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.reactive;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.util.Assert;

/**
 * Used for wrapping a regular {@link org.springframework.cloud.stream.reactive.FluxSender}.
 * The wrapper class exposes the {@link reactor.core.Disposable} object that may be used for cancelling
 * a subscription on the underlying {@link reactor.core.publisher.Flux}.
 *
 * @author Soby Chacko
 * @since 1.2.2
 */
class DisposableFluxSender implements FluxSender {

	private FluxSender wrappedFluxSender;

	private Disposable disposable;

	void setWrappedFluxSender(FluxSender wrappedFluxSender) {
		this.wrappedFluxSender = wrappedFluxSender;
	}

	void setDisposable(Disposable disposable) {
		this.disposable = disposable;
	}

	@Override
	public Mono<Void> send(Flux<?> flux) {
		Assert.state(wrappedFluxSender != null, "No FluxSender is available");
		return this.wrappedFluxSender.send(flux);
	}

	Disposable getDisposable() {
		return disposable;
	}
}
