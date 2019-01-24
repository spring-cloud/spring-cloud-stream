/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.integration.endpoint.AbstractPollingEndpoint;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.util.Assert;

/**
 * A Polling Endpoint that invokes a {@link PollableMessageSource}.
 *
 * @author David Turanski
 **/
public class PollableMessageSourcePoller extends AbstractPollingEndpoint {

	private final MessageHandler messageHandler;

	private final PollableMessageSource messageSource;

	private final ParameterizedTypeReference<?> parameterizedTypeReference;

	private TaskScheduler taskScheduler = new ConcurrentTaskScheduler();

	/**
	 * @param pollerMetadata the {@link PollerMetadata} to configure the poller.
	 * @param messageSource the {@link PollableMessageSource}.
	 * @param messageHandler the {@link MessageHandler} to apply on received messages.
	 * @param parameterizedTypeReference the {@link ParameterizedTypeReference} indicating the target payload type.
	 */
	public PollableMessageSourcePoller(PollerMetadata pollerMetadata, PollableMessageSource messageSource,
		MessageHandler messageHandler, @Nullable ParameterizedTypeReference<?> parameterizedTypeReference) {

		Assert.notNull(pollerMetadata, "'pollerMetadata' cannot be null");
		Assert.notNull(messageSource, "'messageSource' cannot be null");
		Assert.notNull(messageHandler, "'messageHandler' cannot be null");

		this.messageHandler = messageHandler;
		this.messageSource = messageSource;
		this.parameterizedTypeReference = parameterizedTypeReference;
		this.setTrigger(pollerMetadata.getTrigger());
		this.setMaxMessagesPerPoll(pollerMetadata.getMaxMessagesPerPoll());
		this.setAdviceChain(pollerMetadata.getAdviceChain());
		this.setTaskExecutor(pollerMetadata.getTaskExecutor());
		this.setErrorHandler(pollerMetadata.getErrorHandler());
		this.setTransactionSynchronizationFactory(pollerMetadata.getTransactionSynchronizationFactory());
		this.setTaskScheduler(taskScheduler);
	}

	/**
	 *  @param pollerMetadata the {@link PollerMetadata} to configure the poller.
 	 * @param messageSource the {@link PollableMessageSource}.
 	 * @param messageHandler the {@link MessageHandler} to apply on received messages.
 	 */
	public PollableMessageSourcePoller(PollerMetadata pollerMetadata, PollableMessageSource messageSource,
		MessageHandler messageHandler) {
		this(pollerMetadata, messageSource, messageHandler, null);
	}

	@Override
	protected Message<?> receiveMessage() {
		return this.messageSource.receive(this.parameterizedTypeReference);
	}
	@Override
	protected void handleMessage(Message<?> message) {
		messageHandler.handleMessage(message);
	}
}
