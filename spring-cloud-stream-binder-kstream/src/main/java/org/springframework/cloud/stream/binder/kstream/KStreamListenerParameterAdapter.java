/*
 * Copyright 2017-2018 the original author or authors.
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

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KStreamListenerParameterAdapter implements StreamListenerParameterAdapter<KStream<?,?>, KStream<?, ?>> {

	private final KStreamBoundMessageConversionDelegate kStreamBoundMessageConversionDelegate;
	private final KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue;

	public KStreamListenerParameterAdapter(KStreamBoundMessageConversionDelegate kStreamBoundMessageConversionDelegate,
										KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue) {
		this.kStreamBoundMessageConversionDelegate = kStreamBoundMessageConversionDelegate;
		this.KStreamBindingInformationCatalogue = KStreamBindingInformationCatalogue;
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
		if (this.KStreamBindingInformationCatalogue.isUseNativeDecoding(bindingTarget)) {
			return bindingTarget.map((KeyValueMapper) KeyValue::new);
		}
		else {
			return kStreamBoundMessageConversionDelegate.deserializeOnInbound(valueClass, bindingTarget);
		}
	}
}
