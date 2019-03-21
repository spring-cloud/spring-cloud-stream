/*
 * Copyright 2016-2017 the original author or authors.
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

import java.io.Closeable;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Used for {@link org.springframework.cloud.stream.annotation.StreamListener} and
 * {@link org.springframework.cloud.stream.reactive.StreamEmitter} arguments annotated
 * with {@link org.springframework.cloud.stream.annotation.Output}.
 *
 * @author Marius Bogoevici
 * @see reactor.core.Disposable
 */
public interface FluxSender extends Closeable {

	/**
	 * Streams the {@link reactor.core.publisher.Flux} through the binding target
	 * corresponding to the {@link org.springframework.cloud.stream.annotation.Output}
	 * annotation of the argument.
	 * @param flux a {@link Flux} that will be streamed through the binding target
	 * @return a {@link Mono} representing the result of sending the flux (completion or
	 * error)
	 */
	Mono<Void> send(Flux<?> flux);

}
