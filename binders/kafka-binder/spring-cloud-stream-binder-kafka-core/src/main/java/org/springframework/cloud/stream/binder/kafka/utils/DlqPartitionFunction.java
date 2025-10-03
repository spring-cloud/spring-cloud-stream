/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.utils;

import org.apache.commons.logging.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.lang.Nullable;

/**
 * A TriFunction that takes a consumer group, consumer record, and throwable and returns
 * which partition to publish to the dead letter topic. Returning {@code null} means Kafka
 * will choose the partition.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
@FunctionalInterface
public interface DlqPartitionFunction {

	/**
	 * Returns the same partition as the original recor.
	 */
	DlqPartitionFunction ORIGINAL_PARTITION = (group, rec, ex) -> rec.partition();

	/**
	 * Returns 0.
	 */
	DlqPartitionFunction PARTITION_ZERO = (group, rec, ex) -> 0;

	/**
	 * Apply the function.
	 * @param group the consumer group.
	 * @param record the consumer record.
	 * @param throwable the exception.
	 * @return the DLQ partition, or null.
	 */
	@Nullable
	Integer apply(String group, ConsumerRecord<?, ?> record, Throwable throwable);

	/**
	 * Determine the fallback function to use based on the dlq partition count if no
	 * {@link DlqPartitionFunction} bean is provided.
	 * @param dlqPartitions the partition count.
	 * @param logger the logger.
	 * @return the fallback.
	 */
	static DlqPartitionFunction determineFallbackFunction(@Nullable Integer dlqPartitions, Log logger) {
		if (dlqPartitions == null) {
			return ORIGINAL_PARTITION;
		}
		else if (dlqPartitions > 1) {
			logger.error("'dlqPartitions' is > 1 but a custom DlqPartitionFunction bean is not provided");
			return ORIGINAL_PARTITION;
		}
		else {
			return PARTITION_ZERO;
		}
	}

}
