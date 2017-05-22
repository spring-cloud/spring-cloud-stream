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

import java.io.Closeable;

import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;

import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;

/**
 * A {@link StreamListenerResultAdapter} from an {@link Observable} return type to a bound
 * {@link MessageChannel}.
 *
 * @author Marius Bogoevici
 */
public class ObservableToMessageChannelResultAdapter
		implements StreamListenerResultAdapter<Observable<?>, MessageChannel> {

	private PublisherToMessageChannelResultAdapter publisherToMessageChannelResultAdapter;

	public ObservableToMessageChannelResultAdapter(
			PublisherToMessageChannelResultAdapter publisherToMessageChannelResultAdapter) {
		Assert.notNull(publisherToMessageChannelResultAdapter, "cannot be null");
		this.publisherToMessageChannelResultAdapter = publisherToMessageChannelResultAdapter;
	}

	@Override
	public boolean supports(Class<?> resultType, Class<?> bindingTarget) {
		return Observable.class.isAssignableFrom(resultType)
				&& MessageChannel.class.isAssignableFrom(bindingTarget);
	}

	public Closeable adapt(Observable<?> streamListenerResult, MessageChannel bindingTarget) {
		Publisher<?> adaptedPublisher = RxReactiveStreams.toPublisher(streamListenerResult);
		return this.publisherToMessageChannelResultAdapter.adapt(adaptedPublisher, bindingTarget);
	}

}
