/*
 * Copyright 2012-present the original author or authors.
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

package org.springframework.cloud.bus.trace;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * A trace of application event sends and acks. Data from this class is exposed by the
 * {@link org.springframework.cloud.bus.endpoint.TraceBusEndpoint}, usually as JSON.
 *
 * @author Ngoc Nhan
 * @since 5.1.0
 */
public final class Trace {

	private final Instant timestamp;

	private final String signal;

	private final String type;

	private @Nullable String id;

	private @Nullable String origin;

	private @Nullable String destination;

	/**
	 * Primarily for use by {@link TraceRepository} implementations when storing a trace.
	 * @param timestamp the instant that the trace was created (must not be {@code null})
	 * @param signal the signal (must not be {@code null} or empty)
	 * @param type the event type (must not be {@code null} or empty)
	 */
	public Trace(Instant timestamp, String signal, String type) {
		Assert.notNull(timestamp, "timestamp must not be null");
		Assert.hasText(signal, "signal must not be null or empty");
		Assert.hasText(type, "type must not be null or empty");
		this.timestamp = timestamp;
		this.signal = signal;
		this.type = type;
	}

	/**
	 * Returns the timestamp of the trace.
	 * @return the trace timestamp
	 */
	public Instant getTimestamp() {
		return this.timestamp;
	}

	/**
	 * Returns the signal associated with the trace.
	 * @return the trace signal
	 */
	public String getSignal() {
		return signal;
	}

	/**
	 * Returns the event type associated with the trace.
	 * @return the event type
	 */
	public String getType() {
		return this.type;
	}

	/**
	 * Returns the event id associated with the trace.
	 * @return the event id
	 */
	public @Nullable String getId() {
		return this.id;
	}

	/**
	 * Sets the event ID associated with the trace.
	 * @param id the event ID
	 */
	public void setId(@Nullable String id) {
		this.id = id;
	}

	/**
	 * Returns the origin service associated with the trace.
	 * @return the origin service
	 */
	public @Nullable String getOrigin() {
		return this.origin;
	}

	/**
	 * Sets the origin service associated with the trace.
	 * @param origin the origin service
	 */
	public void setOrigin(@Nullable String origin) {
		this.origin = origin;
	}

	/**
	 * Returns the destination service associated with the trace.
	 * @return the destination service
	 */
	public @Nullable String getDestination() {
		return this.destination;
	}

	/**
	 * Sets the destination service associated with the trace.
	 * @param destination the destination service
	 */
	public void setDestination(@Nullable String destination) {
		this.destination = destination;
	}

	@Override
	public boolean equals(Object o) {

		if (!(o instanceof Trace that)) {
			return false;
		}

		return Objects.equals(this.timestamp, that.timestamp) && Objects.equals(this.signal, that.signal)
				&& Objects.equals(this.type, that.type) && Objects.equals(this.id, that.id)
				&& Objects.equals(this.origin, that.origin) && Objects.equals(this.destination, that.destination);
	}

	@Override
	public int hashCode() {

		return Objects.hash(this.timestamp, this.signal, this.type, this.id, this.origin, this.destination);
	}

}
