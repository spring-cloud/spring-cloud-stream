/*
 * Copyright 2024-2024 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * @author Oleg Zhurakousky
 * @since 4.2
 */
public final class StandardBatchUtils {

	private StandardBatchUtils() {

	}

	/**
	 * Iterates over batch message structure returning {@link Iterable} of individual messages.
	 *
	 * @param batchMessage instance  of batch {@link Message}
	 * @return instance of {@link Iterable} representing individual Messages in a batch {@link Message} as {@link Entry}.
	 */
	public static Iterable<Entry<Object, Map<String, Object>>> iterate(Message<List<Object>> batchMessage) {
		return () -> new Iterator<>() {
			int index = 0;

			@Override
			public Entry<Object, Map<String, Object>> next() {
				return getMessageByIndex(batchMessage, index++);
			}

			@Override
			public boolean hasNext() {
				return index < batchMessage.getPayload().size();
			}
		};
	}

	/**
	 * Extracts individual {@link Message} by index from batch {@link Message}.
	 * @param batchMessage instance  of batch {@link Message}
	 * @param index index of individual {@link Message} in a batch
	 * @return individual {@link Message} in a batch {@link Message}
	 */
	public static Entry<Object, Map<String, Object>> getMessageByIndex(Message<List<Object>> batchMessage, int index) {
		Assert.isTrue(index < batchMessage.getPayload().size(), "Index " + index + " is out of bounds as there are only "
				+ batchMessage.getPayload().size() + " messages in a batch.");
		return new Entry<>() {

			@Override
			public Map<String, Object> setValue(Map<String, Object> value) {
				throw new UnsupportedOperationException();
			}

			@SuppressWarnings("unchecked")
			@Override
			public Map<String, Object> getValue() {
				return ((List<Map<String, Object>>) batchMessage.getHeaders().get(BinderHeaders.BATCH_HEADERS)).get(index);
			}

			@Override
			public Object getKey() {
				return batchMessage.getPayload().get(index);
			}
		};
	}

	public static class BatchMessageBuilder {

		private final List<Object> payloads = new ArrayList<>();

		private final List<Map<String, Object>> batchHeaders = new ArrayList<>();

		private final Map<String, Object> headers = new HashMap<>();

		public BatchMessageBuilder addMessage(Object payload, Map<String, Object> batchHeaders) {
			this.payloads.add(payload);
			this.batchHeaders.add(batchHeaders);
			return this;
		}

		public BatchMessageBuilder addRootHeader(String key, Object value) {
			this.headers.put(key, value);
			return this;
		}

		public Message<List<Object>> build() {
			this.headers.put(BinderHeaders.BATCH_HEADERS, this.batchHeaders);
			return MessageBuilder.createMessage(payloads, new MessageHeaders(headers));
		}
	}
}
