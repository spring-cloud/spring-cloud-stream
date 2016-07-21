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

import reactor.adapter.RxJava1Adapter;
import rx.Observable;
import rx.Single;

import org.springframework.cloud.stream.binding.StreamListenerArgumentAdapter;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.messaging.MessageChannel;

/**
 * Adapts an {@link org.springframework.cloud.stream.annotation.Output} annotated
 * {@link ObservableSender} to an outbound {@link MessageChannel}.
 * @author Marius Bogoevici
 */
public class MessageChannelToObservableSenderArgumentAdapter implements
		StreamListenerArgumentAdapter<ObservableSender, MessageChannel> {

	private final MessageChannelToFluxSenderArgumentAdapter messageChannelToFluxSenderArgumentAdapter;

	public MessageChannelToObservableSenderArgumentAdapter(
			MessageChannelToFluxSenderArgumentAdapter messageChannelToFluxSenderArgumentAdapter) {
		this.messageChannelToFluxSenderArgumentAdapter = messageChannelToFluxSenderArgumentAdapter;
	}

	@Override
	public boolean supports(Class<?> boundElementType, MethodParameter methodParameter) {
		ResolvableType type = ResolvableType.forMethodParameter(methodParameter);
		return MessageChannel.class.isAssignableFrom(boundElementType) && ObservableSender.class.isAssignableFrom(
				type.getRawClass());
	}

	@Override
	public ObservableSender adapt(MessageChannel boundElement, MethodParameter parameter) {
		return new ObservableSender() {

			private FluxSender fluxSender = MessageChannelToObservableSenderArgumentAdapter.this
					.messageChannelToFluxSenderArgumentAdapter
					.adapt(boundElement, parameter);

			@Override
			public Single<Void> send(Observable<?> observable) {
				return RxJava1Adapter.publisherToSingle(
						this.fluxSender.send(RxJava1Adapter.observableToFlux(observable)));
			}
		};
	}
}
