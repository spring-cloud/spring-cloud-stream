/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.apache.kafka.streams.kstream.KStream;

import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;

/**
 * {@link StreamListenerParameterAdapter} for KStream.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
class KStreamStreamListenerParameterAdapter
		implements StreamListenerParameterAdapter<KStream<?, ?>, KStream<?, ?>> {

	private final KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate;

	private final KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue;

	KStreamStreamListenerParameterAdapter(
			KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate,
			KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue) {
		this.kafkaStreamsMessageConversionDelegate = kafkaStreamsMessageConversionDelegate;
		this.KafkaStreamsBindingInformationCatalogue = KafkaStreamsBindingInformationCatalogue;
	}

	@Override
	public boolean supports(Class bindingTargetType, MethodParameter methodParameter) {
		return KafkaStreamsBinderUtils.supportsKStream(methodParameter, bindingTargetType);
	}

	@Override
	@SuppressWarnings("unchecked")
	public KStream adapt(KStream<?, ?> bindingTarget, MethodParameter parameter) {
		ResolvableType resolvableType = ResolvableType.forMethodParameter(parameter);
		final Class<?> valueClass = (resolvableType.getGeneric(1).getRawClass() != null)
				? (resolvableType.getGeneric(1).getRawClass()) : Object.class;
		if (this.KafkaStreamsBindingInformationCatalogue
				.isUseNativeDecoding(bindingTarget)) {
			return bindingTarget;
		}
		else {
			return this.kafkaStreamsMessageConversionDelegate
					.deserializeOnInbound(valueClass, bindingTarget);
		}
	}

}
