/*
 * Copyright 2021-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.lang.Nullable;
import org.springframework.util.backoff.BackOff;

/**
 * An extension of {@link ListenerContainerCustomizer} that provides access to dead letter
 * metadata.
 *
 * @author Gary Russell
 * @since 3.2
 *
 */
public interface ListenerContainerWithDlqAndRetryCustomizer
		extends ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> {

	@Override
	default void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName, String group) {
	}

	/**
	 * Configure the container.
	 * @param container the container.
	 * @param destinationName the destination name.
	 * @param group the group.
	 * @param dlqDestinationResolver a destination resolver for the dead letter topic (if
	 * enableDlq).
	 * @param backOff the backOff using retry properties (if configured).
	 * @see #retryAndDlqInBinding(String, String)
	 */
	void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName, String group,
			@Nullable BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> dlqDestinationResolver,
			@Nullable BackOff backOff);

	/**
	 * Return false to move retries and DLQ from the binding to a customized error handler
	 * using the retry metadata and/or a {@code DeadLetterPublishingRecoverer} when
	 * configured via
	 * {@link #configure(AbstractMessageListenerContainer, String, String, BiFunction, BackOff)}.
	 * @param destinationName the destination name.
	 * @param group the group.
	 * @return true to disable retrie in the binding
	 */
	default boolean retryAndDlqInBinding(String destinationName, String group) {
		return true;
	}

}
