/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kstream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KStreamListenerParameterAdapter implements StreamListenerParameterAdapter<KStream<?,?>, KStream<?, ?>> {

	private final MessageConverter messageConverter;

	public KStreamListenerParameterAdapter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	@Override
	public boolean supports(Class bindingTargetType, MethodParameter methodParameter) {
		return KStream.class.isAssignableFrom(bindingTargetType)
				&& KStream.class.isAssignableFrom(methodParameter.getParameterType());
	}

	@Override
	@SuppressWarnings("unchecked")
	public KStream adapt(KStream<?, ?> bindingTarget, MethodParameter parameter) {
		ResolvableType resolvableType = ResolvableType.forMethodParameter(parameter);
		final Class<?> valueClass = (resolvableType.getGeneric(1).getRawClass() != null)
				? (resolvableType.getGeneric(1).getRawClass()) : Object.class;

		return bindingTarget.map((KeyValueMapper) (o, o2) -> {
			KeyValue<Object, Object> keyValue;
			if (valueClass.isAssignableFrom(o2.getClass())) {
				keyValue =  new KeyValue<>(o, o2);
			}
			else if (o2 instanceof Message) {
				if (valueClass.isAssignableFrom(((Message) o2).getPayload().getClass())) {
					keyValue = new KeyValue<>(o, ((Message) o2).getPayload());
				}
				else {
					keyValue = new KeyValue<>(o, messageConverter.fromMessage((Message) o2, valueClass));
				}
			}
			else if(o2 instanceof String || o2 instanceof byte[]) {
				Message<Object> message = MessageBuilder.withPayload(o2).build();
				keyValue =  new KeyValue<>(o, messageConverter.fromMessage(message, valueClass));
			}
			else {
				keyValue =  new KeyValue<>(o, o2);
			}
			return keyValue;
		});
	}

}
