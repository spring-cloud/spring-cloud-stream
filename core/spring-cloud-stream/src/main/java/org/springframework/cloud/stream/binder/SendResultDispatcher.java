/*
 * Copyright 2026-present the original author or authors.
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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;

/**
 * Delivers the outcome of sending a message to the {@link SendResultListener}s of an
 * application. Binders create one per producer binding and call it when the broker
 * acknowledges, or fails, a message. Exceptions thrown by a listener are logged and
 * never propagate to the caller, which is typically a client I/O thread.
 *
 * @author Oleg Zhurakousky
 * @since 5.1.1
 */
public final class SendResultDispatcher {

	private static final Log logger = LogFactory.getLog(SendResultDispatcher.class);

	private final String bindingName;

	private final List<SendResultListener> listeners;

	/**
	 * Create a dispatcher.
	 * @param bindingName the name of the producer binding
	 * @param listeners the listeners to notify, in the order they are to be invoked
	 */
	public SendResultDispatcher(String bindingName, List<SendResultListener> listeners) {
		this.bindingName = bindingName;
		this.listeners = List.copyOf(listeners);
	}

	/**
	 * Notify the listeners that a message was acknowledged by the broker.
	 * @param message the message that was sent
	 * @param result binder specific information about the outcome, may be {@code null}
	 */
	public void success(Message<?> message, @Nullable Object result) {
		for (SendResultListener listener : this.listeners) {
			try {
				listener.onSuccess(this.bindingName, message, result);
			}
			catch (Exception ex) {
				logger.warn("SendResultListener " + listener + " failed in onSuccess for binding '"
						+ this.bindingName + "'", ex);
			}
		}
	}

	/**
	 * Notify the listeners that a message could not be sent.
	 * @param message the message that failed
	 * @param exception the cause of the failure
	 */
	public void failure(Message<?> message, Throwable exception) {
		for (SendResultListener listener : this.listeners) {
			try {
				listener.onError(this.bindingName, message, exception);
			}
			catch (Exception ex) {
				logger.warn("SendResultListener " + listener + " failed in onError for binding '"
						+ this.bindingName + "'", ex);
			}
		}
	}

}
