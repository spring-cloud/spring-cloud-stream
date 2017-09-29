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

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Marius Bogoevici
 */
public class KStreamStreamListenerResultAdapter implements StreamListenerResultAdapter<KStream, KStreamBoundElementFactory.KStreamWrapper> {

	@Override
	public boolean supports(Class<?> resultType, Class<?> boundElement) {
		return KStream.class.isAssignableFrom(resultType) && KStream.class.isAssignableFrom(boundElement);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Closeable adapt(KStream streamListenerResult, KStreamBoundElementFactory.KStreamWrapper boundElement) {
		boundElement.wrap(streamListenerResult.map((k, v) -> {
			if (v instanceof Message<?>) {
				return new KeyValue<>(k, v);
			}
			else {
				return new KeyValue<>(k, MessageBuilder.withPayload(v).build());
			}
		}));
		return new NoOpCloseable();
	}

	private static final class NoOpCloseable implements Closeable {

		@Override
		public void close() throws IOException {

		}

	}
}
