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

import java.util.function.BooleanSupplier;

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

	private  final BooleanSupplier receiveCondition;

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * @param pollerMetadata             the {@link PollerMetadata} to configure the poller.
	 * @param messageSource              the {@link PollableMessageSource}.
	 * @param messageHandler             the {@link MessageHandler} to apply on received messages.
	 * @param parameterizedTypeReference the {@link ParameterizedTypeReference} indicating the target payload type.
	 * @param taskScheduler              the {@link TaskScheduler} to use.
	 * @param receiveCondition           the receive condition.
	 */
	private PollableMessageSourcePoller(PollerMetadata pollerMetadata, PollableMessageSource messageSource,
		MessageHandler messageHandler, @Nullable ParameterizedTypeReference<?> parameterizedTypeReference,
		TaskScheduler taskScheduler, BooleanSupplier receiveCondition) {

		Assert.notNull(pollerMetadata, "'pollerMetadata' cannot be null");
		Assert.notNull(messageSource, "'messageSource' cannot be null");
		Assert.notNull(messageHandler, "'messageHandler' cannot be null");
		Assert.notNull(taskScheduler, "'taskScheduler' cannot be null");
		Assert.notNull(receiveCondition, "'receiveCondition cannot be null");

		this.messageHandler = messageHandler;
		this.messageSource = messageSource;
		this.parameterizedTypeReference = parameterizedTypeReference;
		this.receiveCondition = receiveCondition;
		this.setTrigger(pollerMetadata.getTrigger());
		this.setMaxMessagesPerPoll(pollerMetadata.getMaxMessagesPerPoll());
		this.setAdviceChain(pollerMetadata.getAdviceChain());
		this.setTaskExecutor(pollerMetadata.getTaskExecutor());
		this.setErrorHandler(pollerMetadata.getErrorHandler());
		this.setTransactionSynchronizationFactory(pollerMetadata.getTransactionSynchronizationFactory());
		this.setTaskScheduler(taskScheduler);
	}

	@Override
	protected Message<?> receiveMessage() {
		return receiveCondition.getAsBoolean() ?
			this.messageSource.receive(this.parameterizedTypeReference) :
			null;
	}

	@Override
	protected void handleMessage(Message<?> message) {
		messageHandler.handleMessage(message);
	}

	public static class Builder {

		private MessageHandler messageHandler;

		private PollableMessageSource messageSource;

		private ParameterizedTypeReference<?> parameterizedTypeReference;

		private TaskScheduler taskScheduler = new ConcurrentTaskScheduler();

		private BooleanSupplier receiveCondition = () -> true;

		private PollerMetadata pollerMetadata;

		private Builder() {
		}

		/**
		 * Set the {@link MessageHandler} to invoke on received messages.
		 *
		 * @param messageHandler the message handler.
		 * @return the Builder instance.
		 */
		public Builder messageHandler(MessageHandler messageHandler) {
			this.messageHandler = messageHandler;
			return this;
		}

		/**
		 * Set the {@link PollableMessageSource}>
		 *
		 * @param messageSource the message source.
		 * @return the Builder instance.
		 */
		public Builder messageSource(PollableMessageSource messageSource) {
			this.messageSource = messageSource;
			return this;
		}

		/**
		 * Set a {@link PollerMetadata} to configure the poller.
		 *
		 * @param pollerMetadata
		 * @return the Builder instance.
		 */
		public Builder pollerMetadata(PollerMetadata pollerMetadata) {
			this.pollerMetadata = pollerMetadata;
			return this;
		}

		/**
		 * Set a {@link BooleanSupplier} as a condition for receiving messages.
		 *
		 * @param receiveCondition the receive condition.
		 * @return the Builder instance.
		 */
		public Builder receiveCondition(BooleanSupplier receiveCondition) {
			this.receiveCondition = receiveCondition;
			return this;
		}

		/**
		 * Set a {@link ParameterizedTypeReference} indicating the target payload type.
		 *
		 * @param parameterizedTypeReference the type.
		 * @return the Builder instance.
		 */
		public Builder parameterizedTypeReference(ParameterizedTypeReference<?> parameterizedTypeReference) {
			this.parameterizedTypeReference = parameterizedTypeReference;
			return this;
		}

		/**
		 * Set a {@link TaskScheduler} the default is {@link ConcurrentTaskScheduler}.
		 *
		 * @param taskScheduler the taskScheduler.
		 * @return the Builder instance.
		 */
		public Builder taskScheduler(TaskScheduler taskScheduler) {
			this.taskScheduler = taskScheduler;
			return this;
		}

		public PollableMessageSourcePoller build() {
			return new PollableMessageSourcePoller(
				this.pollerMetadata,
				this.messageSource,
				this.messageHandler,
				this.parameterizedTypeReference,
				this.taskScheduler,
				this.receiveCondition);
		}

	}
}
