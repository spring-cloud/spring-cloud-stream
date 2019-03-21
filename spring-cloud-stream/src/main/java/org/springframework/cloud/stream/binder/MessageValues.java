/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * A mutable type for allowing {@link Binder} implementations to transform and enrich
 * message content more efficiently.
 *
 * @author David Turanski
 * @author Marius Bogoevici
 */
public class MessageValues implements Map<String, Object> {

	private Map<String, Object> headers = new HashMap<>();

	private Object payload;

	/**
	 * Create an instance from a {@link Message}.
	 * @param message the message
	 */
	public MessageValues(Message<?> message) {
		this.payload = message.getPayload();
		for (Map.Entry<String, Object> header : message.getHeaders().entrySet()) {
			this.headers.put(header.getKey(), header.getValue());
		}
	}

	public MessageValues(Object payload, Map<String, Object> headers) {
		this.payload = payload;
		this.headers.putAll(headers);
	}

	/**
	 * @return the payload
	 */
	public Object getPayload() {
		return this.payload;
	}

	/**
	 * Set the payload.
	 * @param payload any non null object.
	 */
	public void setPayload(Object payload) {
		Assert.notNull(payload, "'payload' cannot be null");
		this.payload = payload;
	}

	public Map<String, Object> getHeaders() {
		return this.headers;
	}

	/**
	 * Convert to a {@link Message} using a
	 * {@link org.springframework.integration.support.MessageBuilderFactory}.
	 * @param messageBuilderFactory the MessageBuilderFactory
	 * @return the Message
	 */
	public Message<?> toMessage(MessageBuilderFactory messageBuilderFactory) {
		return messageBuilderFactory.withPayload(this.payload).copyHeaders(this.headers)
				.build();
	}

	/**
	 * Convert to a {@link Message} using a the default
	 * {@link org.springframework.integration.support.MessageBuilder}.
	 * @return the Message
	 */
	public Message<?> toMessage() {
		return MessageBuilder.withPayload(this.payload).copyHeaders(this.headers).build();
	}

	@Override
	public int size() {
		return this.headers.size();
	}

	@Override
	public boolean isEmpty() {
		return this.headers.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return this.headers.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return this.headers.containsValue(value);
	}

	@Override
	public Object get(Object key) {
		return this.headers.get(key);
	}

	@Override
	public Object put(String key, Object value) {
		return this.headers.put(key, value);
	}

	@Override
	public Object remove(Object key) {
		return this.headers.remove(key);
	}

	@Override
	public void putAll(Map<? extends String, ?> m) {
		this.headers.putAll(m);
	}

	@Override
	public void clear() {
		this.headers.clear();
	}

	@Override
	public Set<String> keySet() {
		return this.headers.keySet();
	}

	@Override
	public Collection<Object> values() {
		return this.headers.values();
	}

	@Override
	public Set<Entry<String, Object>> entrySet() {
		return this.headers.entrySet();
	}

	public void copyHeadersIfAbsent(Map<String, Object> headersToCopy) {
		for (Entry<String, Object> headersToCopyEntry : headersToCopy.entrySet()) {
			if (!containsKey(headersToCopyEntry.getKey())) {
				put(headersToCopyEntry.getKey(), headersToCopyEntry.getValue());
			}
		}
	}

}
