/*
 * Copyright 2016 the original author or authors.
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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Single;

import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;

/**
 * Adapts an {@link org.springframework.cloud.stream.annotation.Output} annotated
 * {@link ObservableSender} to an outbound {@link MessageChannel}.
 * @author Marius Bogoevici
 */
public class MessageChannelToObservableSenderParameterAdapter implements
		StreamListenerParameterAdapter<ObservableSender, MessageChannel> {

	private final MessageChannelToFluxSenderParameterAdapter messageChannelToFluxSenderArgumentAdapter;

	public MessageChannelToObservableSenderParameterAdapter(
			MessageChannelToFluxSenderParameterAdapter messageChannelToFluxSenderArgumentAdapter) {
		Assert.notNull(messageChannelToFluxSenderArgumentAdapter, "cannot be null");
		this.messageChannelToFluxSenderArgumentAdapter = messageChannelToFluxSenderArgumentAdapter;
	}

	@Override
	public boolean supports(Class<?> bindingTargetType, MethodParameter methodParameter) {
		ResolvableType type = ResolvableType.forMethodParameter(methodParameter);
		return MessageChannel.class.isAssignableFrom(bindingTargetType)
				&& ObservableSender.class.isAssignableFrom(type.getRawClass());
	}

	@Override
	public ObservableSender adapt(MessageChannel bindingTarget, MethodParameter parameter) {
		return new ObservableSender() {

			private FluxSender fluxSender = MessageChannelToObservableSenderParameterAdapter.this.messageChannelToFluxSenderArgumentAdapter
					.adapt(bindingTarget, parameter);

			@Override
			public Single<Void> send(Observable<?> observable) {
				Publisher<?> adaptedPublisher = RxReactiveStreams.toPublisher(observable);
				return RxReactiveStreams.toSingle(
						this.fluxSender.send(Flux.from(adaptedPublisher)));
			}
		};
	}
}
