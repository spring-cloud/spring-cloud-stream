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

import reactor.core.publisher.Flux;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.binding.StreamListenerArgumentAdapter;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.converter.CompositeMessageConverter;

/**
 * Adapts an {@link org.springframework.cloud.stream.annotation.Input} annotated
 * {@link MessageChannel} to a {@link Flux}.
 * @author Marius Bogoevici
 */
public class MessageChannelToInputFluxArgumentAdapter
		implements StreamListenerArgumentAdapter<Flux<?>, SubscribableChannel> {

	private CompositeMessageConverter messageConverter;

	public MessageChannelToInputFluxArgumentAdapter(CompositeMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	@Override
	public boolean supports(Class<?> boundElementType, MethodParameter methodParameter) {
		return SubscribableChannel.class.isAssignableFrom(boundElementType)
				&& methodParameter.getParameterAnnotation(Input.class) != null
				&& Flux.class.isAssignableFrom(methodParameter.getParameterType());
	}

	@Override
	public Flux<?> adapt(final SubscribableChannel boundElement, MethodParameter parameter) {
		ResolvableType resolvableType = ResolvableType.forMethodParameter(parameter);
		Class<?> argumentClass = resolvableType.getGeneric(0).getRawClass();
		final Object monitor = new Object();
		if (Message.class.isAssignableFrom(argumentClass)) {
			return Flux.create(emitter -> {
				MessageHandler messageHandler = message -> {
					synchronized (monitor) {
						emitter.next(message);
					}
				};
				boundElement.subscribe(messageHandler);
				emitter.setCancellation(() -> boundElement.unsubscribe(messageHandler));
			});
		}
		else {
			return Flux.create(emitter -> {
				MessageHandler messageHandler = message -> {
					synchronized (monitor) {
						if (argumentClass.isAssignableFrom(message.getPayload().getClass())) {
							emitter.next(message.getPayload());
						}
						else {
							emitter.next(this.messageConverter.fromMessage(message, argumentClass));
						}
					}
				};
				boundElement.subscribe(messageHandler);
				emitter.setCancellation(() -> boundElement.unsubscribe(messageHandler));
			});
		}
	}
}
