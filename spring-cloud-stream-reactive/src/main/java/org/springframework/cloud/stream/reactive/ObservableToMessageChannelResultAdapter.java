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
import reactor.core.publisher.Flux;
import rx.Observable;

import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.messaging.MessageChannel;

/**
 * A {@link StreamListenerResultAdapter} from a {@link Flux}
 * return type to a bound {@link MessageChannel}.
 * @author Marius Bogoevici
 */
public class ObservableToMessageChannelResultAdapter
		implements StreamListenerResultAdapter<Observable<?>, MessageChannel> {

	private FluxToMessageChannelResultAdapter fluxToMessageChannelResultAdapter;

	public ObservableToMessageChannelResultAdapter(
			FluxToMessageChannelResultAdapter fluxToMessageChannelResultAdapter) {
		this.fluxToMessageChannelResultAdapter = fluxToMessageChannelResultAdapter;
	}

	@Override
	public boolean supports(Class<?> resultType, Class<?> boundType) {
		return Observable.class.isAssignableFrom(resultType) && MessageChannel.class.isAssignableFrom(boundType);
	}

	public void adapt(Observable<?> streamListenerResult, MessageChannel boundElement) {
		this.fluxToMessageChannelResultAdapter.adapt(RxJava1Adapter.observableToFlux(streamListenerResult),
				boundElement);
	}
}
