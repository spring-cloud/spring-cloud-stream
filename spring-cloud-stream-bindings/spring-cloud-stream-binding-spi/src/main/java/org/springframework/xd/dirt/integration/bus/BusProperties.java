/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.bus;


/**
 * Common bus properties.
 *
 * @author Gary Russell
 */
public interface BusProperties {

	/**
	 * The retry back off initial interval.
	 */
	public static final String BACK_OFF_INITIAL_INTERVAL = "backOffInitialInterval";

	/**
	 * The retry back off max interval.
	 */
	public static final String BACK_OFF_MAX_INTERVAL = "backOffMaxInterval";

	/**
	 * The retry back off multiplier.
	 */
	public static final String BACK_OFF_MULTIPLIER = "backOffMultiplier";

	/**
	 * The minimum number of concurrent deliveries.
	 */
	public static final String CONCURRENCY = "concurrency";

	/**
	 * The maximum delivery attempts when a delivery fails.
	 */
	public static final String MAX_ATTEMPTS = "maxAttempts";

	/**
	 * The maximum number of concurrent deliveries.
	 */
	public static final String MAX_CONCURRENCY = "maxConcurrency";

	/**
	 *  The sequence index of the module.
	 *  In a partitioned stream, it is identical to the partition index.
	 */
	public static final String SEQUENCE = "sequence";

	/**
	 *  The number of consumers, i.e. module instances in the stream.
	 *  In a partitioned stream, it is identical to the partition count.
	 */
	public static final String COUNT = "count";

	/**
	 * The consumer's partition index.
	 */
	public static final String PARTITION_INDEX = "partitionIndex";

	/**
	 * The partition key expression.
	 */
	public static final String PARTITION_KEY_EXPRESSION = "partitionKeyExpression";

	/**
	 * The partition key class.
	 */
	public static final String PARTITION_KEY_EXTRACTOR_CLASS = "partitionKeyExtractorClass";

	/**
	 * The partition selector class.
	 */
	public static final String PARTITION_SELECTOR_CLASS = "partitionSelectorClass";

	/**
	 * The partition selector expression.
	 */
	public static final String PARTITION_SELECTOR_EXPRESSION = "partitionSelectorExpression";

	/**
	 * If true, the bus will attempt to create a direct binding between the producer and consumer.
	 */
	public static final String DIRECT_BINDING_ALLOWED = "directBindingAllowed";

	/**
	 * True if message batching is enabled.
	 */
	public static final String BATCHING_ENABLED = "batchingEnabled";

	/**
	 * The batch size if batching is enabled.
	 */
	public static final String BATCH_SIZE = "batchSize";

	/**
	 * The buffer limit if batching is enabled.
	 */
	public static final String BATCH_BUFFER_LIMIT = "batchBufferLimit";

	/**
	 * The batch timeout if batching is enabled.
	 */
	public static final String BATCH_TIMEOUT = "batchTimeout";

	/**
	 * For all non-terminal modules, the number of modules coming after this one, irrespective of partitioning.
	 */
	public static final String NEXT_MODULE_COUNT = "next.module.count";

	/**
	 * For all non-terminal modules, the concurrency for module coming after this one.
	 */
	public static final String NEXT_MODULE_CONCURRENCY = "next.module.concurrency";

	/**
	 * Compression enabled.
	 */
	public static final String COMPRESS = "compress";

	/**
	 * Durable pub/sub consumer.
	 */
	public static final String DURABLE = "durableSubscription";

	/**
	 * Minimum partition count, if the transport supports partitioning natively (e.g. Kafka)
	 */
	public static final String MIN_PARTITION_COUNT = "minPartitionCount";

}
