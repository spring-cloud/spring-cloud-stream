/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * A rebalance listener that provides access to the binding name consumer object. It can
 * be used to perform seek operations on the consumer after a rebalance.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
public interface KafkaBindingRebalanceListener {

	/**
	 * Invoked by the container before any pending offsets are committed.
	 * @param bindingName the name of the binding.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 */
	default void onPartitionsRevokedBeforeCommit(String bindingName,
			Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		// do nothing
	}

	/**
	 * Invoked by the container after any pending offsets are committed.
	 * @param bindingName the name of the binding.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 */
	default void onPartitionsRevokedAfterCommit(String bindingName,
			Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		// do nothing
	}

	/**
	 * Invoked when partitions are initially assigned or after a rebalance. Applications
	 * might only want to perform seek operations on an initial assignment.
	 * @param bindingName the name of the binding.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 * @param initial true if this is the initial assignment.
	 */
	default void onPartitionsAssigned(String bindingName, Consumer<?, ?> consumer,
			Collection<TopicPartition> partitions, boolean initial) {
		// do nothing
	}

}
