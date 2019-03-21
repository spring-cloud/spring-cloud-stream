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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

/**
 * A {@link org.springframework.cloud.stream.binding.StreamListenerResultAdapter} from a
 * {@link Publisher} return type to a bound {@link MessageChannel}.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 *
 */
public class PublisherToMessageChannelResultAdapter
		implements StreamListenerResultAdapter<Publisher<?>, MessageChannel> {

	private Log log = LogFactory.getLog(PublisherToMessageChannelResultAdapter.class);

	@Override
	public boolean supports(Class<?> resultType, Class<?> bindingTarget) {
		return Publisher.class.isAssignableFrom(resultType)
				&& MessageChannel.class.isAssignableFrom(bindingTarget);
	}

	public Closeable adapt(Publisher<?> streamListenerResult,
			MessageChannel bindingTarget) {
		Disposable disposable = Flux.from(streamListenerResult)
				.doOnError(e -> this.log.error("Error while processing result", e))
				.retry()
				.subscribe(result -> bindingTarget
						.send(result instanceof Message<?> ? (Message<?>) result
								: MessageBuilder.withPayload(result).build()));

		return disposable::dispose;
	}

}
