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

package org.springframework.cloud.stream.binder.gemfire;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.integration.endpoint.ExpressionMessageProducerSupport;
import org.springframework.integration.gemfire.inbound.CacheListeningMessageProducer;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * An inbound endpoint that implements GemFire's {@link AsyncEventListener}
 * interface. This implementation differs from {@link CacheListeningMessageProducer}
 * because it uses an {@link AsyncEventQueue} to offload processing to a dedicated thread.
 * <p>
 * The default supported operations are
 * <ul>
 *     <li>{@link Operation#CREATE}</li>
 *     <li>{@link Operation#UPDATE}</li>
 *     <li>{@link Operation#PUTALL_CREATE}</li>
 *     <li>{@link Operation#PUTALL_UPDATE}</li>
 * </ul>
 * A SpEL expression may be provided to generate a Message payload by evaluating
 * that expression against the {@link AsyncEvent} instance as the root object.
 * If no {@code payloadExpression} is provided, the {@code AsyncEvent} itself
 * will be the payload.
 * <p>
 *
 * @author Patrick Peralta
 */
public class AsyncEventListeningMessageProducer extends ExpressionMessageProducerSupport implements AsyncEventListener {

	private static final Logger logger = LoggerFactory.getLogger(AsyncEventListeningMessageProducer.class);

	private volatile Set<Operation> supportedOperations =
			new HashSet<Operation>(Arrays.asList(
					Operation.CREATE,
					Operation.UPDATE,
					Operation.PUTALL_CREATE,
					Operation.PUTALL_UPDATE));


	/**
	 * Set the list of operations that will cause a message to be published.
	 *
	 * @param operations supported operations
	 */
	public void setSupportedOperations(Operation... operations) {
		Assert.notEmpty(operations, "operations must not be empty");
		this.supportedOperations = new HashSet<Operation>(Arrays.asList(operations));
	}

	@Override
	public String getComponentType() {
		return "gemfire:inbound-channel-adapter";
	}

	@Override
	public boolean processEvents(List<AsyncEvent> events) {
		for (AsyncEvent event : events) {
			if (this.supportedOperations.contains(event.getOperation())) {
				processEvent(event);
			}
		}
		return true;
	}

	private void processEvent(AsyncEvent event) {
		this.publish(evaluatePayloadExpression(event));
	}

	private void publish(Object object) {
		Message<?> message = object instanceof Message
				? (Message<?>) object
				: getMessageBuilderFactory().withPayload(object).build();
		sendMessage(message);
	}

	@Override
	public void close() {
	}

}
