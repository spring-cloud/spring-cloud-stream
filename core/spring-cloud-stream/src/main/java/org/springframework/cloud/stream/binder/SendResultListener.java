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

import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;

/**
 * Callback that is notified of the outcome of sending a message through a producer
 * binding, including messages sent with
 * {@link org.springframework.cloud.stream.function.StreamBridge}.
 * <p>
 * By default sending is "fire and forget": {@code StreamBridge.send(..)} only reports
 * whether the message was accepted by the binder. When a bean of this type is present
 * in the application context, binders that can report the outcome of the send
 * (currently the Kafka binder) invoke it once the broker has acknowledged the message
 * or the send has failed.
 * <p>
 * Implementations are invoked from the thread that completes the send (for example the
 * Kafka producer I/O thread), so they should return quickly and must not block. Any
 * exception thrown by an implementation is logged and otherwise ignored. The message
 * passed to the callbacks is the message as it was handed to the binder, that is, after
 * payload conversion and with the headers that were set for sending. To correlate a
 * result with the business action that produced it, set a header on the message that is
 * sent and read it back here.
 *
 * @author Oleg Zhurakousky
 * @since 5.1.1
 */
public interface SendResultListener {

	/**
	 * Invoked when the broker has acknowledged the message.
	 * @param bindingName the name of the producer binding the message was sent to
	 * @param message the message that was sent
	 * @param result binder specific information about the outcome (for the Kafka binder
	 * the {@code RecordMetadata} of the written record), or {@code null} if the binder
	 * does not provide any
	 */
	default void onSuccess(String bindingName, Message<?> message, @Nullable Object result) {
	}

	/**
	 * Invoked when sending the message failed.
	 * @param bindingName the name of the producer binding the message was sent to
	 * @param message the message that could not be sent
	 * @param exception the cause of the failure
	 */
	default void onError(String bindingName, Message<?> message, Throwable exception) {
	}

}
