/*
 * Copyright 2016-2018 the original author or authors.
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

import org.springframework.expression.EvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * Utility class to determine if a binding is configured for partitioning (based on the
 * binder properties provided in the constructor) and what partition a message should be
 * delivered to.
 *
 * @author Patrick Peralta
 * @author David Turanski
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Mark Fisher
 * @author Marius Bogoevici
 */
public class PartitionHandler {

	private final EvaluationContext evaluationContext;

	private final ProducerProperties producerProperties;

	private final PartitionKeyExtractorStrategy partitionKeyExtractorStrategy;

	private final PartitionSelectorStrategy partitionSelectorStrategy;

	private volatile int partitionCount;

	/**
	 * Construct a {@code PartitionHandler}.
	 * @param evaluationContext evaluation context for binder
	 * @param properties binder properties
	 * @param partitionKeyExtractorStrategy PartitionKeyExtractor strategy
	 * @param partitionSelectorStrategy PartitionSelector strategy
	 */
	public PartitionHandler(EvaluationContext evaluationContext,
			ProducerProperties properties,
			PartitionKeyExtractorStrategy partitionKeyExtractorStrategy,
			PartitionSelectorStrategy partitionSelectorStrategy) {
		this.evaluationContext = evaluationContext;
		this.producerProperties = properties;
		this.partitionKeyExtractorStrategy = partitionKeyExtractorStrategy;
		this.partitionSelectorStrategy = partitionSelectorStrategy;
		this.partitionCount = this.producerProperties.getPartitionCount();
	}

	/**
	 * Set the actual partition count (if different to the configured count).
	 * @param partitionCount the count.
	 */
	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}

	/**
	 * Determine the partition to which to send this message.
	 * <p>
	 * If a partition key extractor class is provided, it is invoked to determine the key.
	 * Otherwise, the partition key expression is evaluated to obtain the key value.
	 * <p>
	 * If a partition selector class is provided, it will be invoked to determine the
	 * partition. Otherwise, if the partition expression is not null, it is evaluated
	 * against the key and is expected to return an integer to which the modulo function
	 * will be applied, using the {@code partitionCount} as the divisor. If no partition
	 * expression is provided, the key will be passed to the binder partition strategy
	 * along with the {@code partitionCount}. The default partition strategy uses
	 * {@code key.hashCode()}, and the result will be the mod of that value.
	 * @param message the message.
	 * @return the partition
	 */
	public int determinePartition(Message<?> message) {
		Object key = extractKey(message);

		int partition;
		if (this.producerProperties.getPartitionSelectorExpression() != null) {
			partition = this.producerProperties.getPartitionSelectorExpression()
					.getValue(this.evaluationContext, key, Integer.class);
		}
		else {
			partition = this.partitionSelectorStrategy.selectPartition(key,
					this.partitionCount);
		}
		// protection in case a user selector returns a negative.
		return Math.abs(partition % this.partitionCount);
	}

	private Object extractKey(Message<?> message) {
		Object key = invokeKeyExtractor(message);
		if (key == null && this.producerProperties.getPartitionKeyExpression() != null) {
			key = this.producerProperties.getPartitionKeyExpression()
					.getValue(this.evaluationContext, message);
		}
		Assert.notNull(key, "Partition key cannot be null");

		return key;
	}

	private Object invokeKeyExtractor(Message<?> message) {
		if (this.partitionKeyExtractorStrategy != null) {
			return this.partitionKeyExtractorStrategy.extractKey(message);
		}
		return null;
	}

}
