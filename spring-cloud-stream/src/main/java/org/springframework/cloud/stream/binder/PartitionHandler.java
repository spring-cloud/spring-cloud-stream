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

package org.springframework.cloud.stream.binder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Utility class to determine if a binding is configured for partitioning
 * (based on the binder properties provided in the constructor) and
 * what partition a message should be delivered to.
 *
 * @author Patrick Peralta
 * @author David Turanski
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Mark Fisher
 * @author Marius Bogoevici
 */
public class PartitionHandler {
	private static final Logger logger = LoggerFactory.getLogger(PartitionHandler.class);

	private final ConfigurableListableBeanFactory beanFactory;

	private final EvaluationContext evaluationContext;

	private final PartitionSelectorStrategy partitionSelector;

	private final PartitioningMetadata partitioningMetadata;


	/**
	 * Construct a {@code PartitionHandler}.
	 *
	 * @param beanFactory bean factory for binder
	 * @param evaluationContext evaluation context for binder
	 * @param partitionSelector configured partition selector; may be {@code null}
	 * @param properties binder properties
	 * @param partitionCount number of partitions configured for binder
	 */
	public PartitionHandler(ConfigurableListableBeanFactory beanFactory,
			EvaluationContext evaluationContext,
			PartitionSelectorStrategy partitionSelector,
			DefaultBindingPropertiesAccessor properties, int partitionCount) {
		this.beanFactory = beanFactory;
		this.evaluationContext = evaluationContext;
		this.partitionSelector = partitionSelector == null
				? new DefaultPartitionSelector()
				: partitionSelector;
		this.partitioningMetadata = new PartitioningMetadata(properties, partitionCount);
	}

	/**
	 * Return {@code true} if the binder properties provided indicate
	 * that this binder is configured for partitioning.
	 *
	 * @return true if partitioning is enabled
	 */
	public boolean isPartitionedModule() {
		return this.partitioningMetadata.isPartitionedModule();
	}

	/**
	 * Determine the partition to which to send this message.
	 * <p>
	 * If a partition key extractor class is provided, it is invoked to determine
	 * the key. Otherwise, the partition key expression is evaluated to obtain the
	 * key value.
	 * <p>
	 * If a partition selector class is provided, it will be invoked to determine the
	 * partition. Otherwise, if the partition expression is not null, it is evaluated
	 * against the key and is expected to return an integer to which the modulo
	 * function will be applied, using the {@code partitionCount} as the divisor. If no
	 * partition expression is provided, the key will be passed to the binder
	 * partition strategy along with the {@code partitionCount}. The default partition
	 * strategy uses {@code key.hashCode()}, and the result will be the mod of that value.
	 *
	 * @param message the message.
	 * @return the partition
	 */
	public int determinePartition(Message<?> message) {
		Object key = null;
		if (StringUtils.hasText(partitioningMetadata.partitionKeyExtractorClass)) {
			key = invokeExtractor(partitioningMetadata.partitionKeyExtractorClass, message);
		}
		else if (partitioningMetadata.partitionKeyExpression != null) {
			key = partitioningMetadata.partitionKeyExpression.getValue(this.evaluationContext, message);
		}
		Assert.notNull(key, "Partition key cannot be null");
		int partition;
		if (StringUtils.hasText(partitioningMetadata.partitionSelectorClass)) {
			partition = invokePartitionSelector(partitioningMetadata.partitionSelectorClass,
					key, partitioningMetadata.partitionCount);
		}
		else if (partitioningMetadata.partitionSelectorExpression != null) {
			partition = partitioningMetadata.partitionSelectorExpression.getValue(this.evaluationContext, key, Integer.class);
		}
		else {
			partition = this.partitionSelector.selectPartition(key, partitioningMetadata.partitionCount);
		}
		partition = partition % partitioningMetadata.partitionCount;
		if (partition < 0) { // protection in case a user selector returns a negative.
			partition = Math.abs(partition);
		}
		return partition;
	}

	private Object invokeExtractor(String partitionKeyExtractorClassName, Message<?> message) {
		if (this.beanFactory.containsBean(partitionKeyExtractorClassName)) {
			return this.beanFactory.getBean(partitionKeyExtractorClassName,
					PartitionKeyExtractorStrategy.class)
					.extractKey(message);
		}
		Class<?> clazz;
		try {
			clazz = ClassUtils.forName(partitionKeyExtractorClassName,
					this.beanFactory.getBeanClassLoader());
		}
		catch (Exception e) {
			logger.error("Failed to load key extractor", e);
			throw new BinderException("Failed to load key extractor: " + partitionKeyExtractorClassName, e);
		}
		try {
			Object extractor = clazz.newInstance();
			Assert.isInstanceOf(PartitionKeyExtractorStrategy.class, extractor);
			this.beanFactory.registerSingleton(partitionKeyExtractorClassName, extractor);
			this.beanFactory.initializeBean(extractor, partitionKeyExtractorClassName);
			return ((PartitionKeyExtractorStrategy) extractor).extractKey(message);
		}
		catch (Exception e) {
			logger.error("Failed to instantiate key extractor", e);
			throw new BinderException("Failed to instantiate key extractor: " + partitionKeyExtractorClassName, e);
		}
	}

	private int invokePartitionSelector(String partitionSelectorClassName, Object key, int partitionCount) {
		if (this.beanFactory.containsBean(partitionSelectorClassName)) {
			return this.beanFactory.getBean(partitionSelectorClassName, PartitionSelectorStrategy.class)
					.selectPartition(key, partitionCount);
		}
		Class<?> clazz;
		try {
			clazz = ClassUtils.forName(partitionSelectorClassName, this.beanFactory.getBeanClassLoader());
		}
		catch (Exception e) {
			logger.error("Failed to load partition selector", e);
			throw new BinderException("Failed to load partition selector: " + partitionSelectorClassName, e);
		}
		try {
			Object extractor = clazz.newInstance();
			Assert.isInstanceOf(PartitionKeyExtractorStrategy.class, extractor);
			this.beanFactory.registerSingleton(partitionSelectorClassName, extractor);
			this.beanFactory.initializeBean(extractor, partitionSelectorClassName);
			return ((PartitionSelectorStrategy) extractor).selectPartition(key, partitionCount);
		}
		catch (Exception e) {
			logger.error("Failed to instantiate partition selector", e);
			throw new BinderException("Failed to instantiate partition selector: " + partitionSelectorClassName,
					e);
		}
	}

	/**
	 * Default partition strategy; only works on keys with "real" hash codes,
	 * such as String. Caller now always applies modulo so no need to do so here.
	 */
	private static class DefaultPartitionSelector implements PartitionSelectorStrategy {

		@Override
		public int selectPartition(Object key, int partitionCount) {
			int hashCode = key.hashCode();
			if (hashCode == Integer.MIN_VALUE) {
				hashCode = 0;
			}
			return Math.abs(hashCode);
		}

	}

	private static class PartitioningMetadata {

		private final String partitionKeyExtractorClass;

		private final Expression partitionKeyExpression;

		private final String partitionSelectorClass;

		private final Expression partitionSelectorExpression;

		private final int partitionCount;

		public PartitioningMetadata(DefaultBindingPropertiesAccessor properties, int partitionCount) {
			this.partitionCount = partitionCount;
			this.partitionKeyExtractorClass = properties.getPartitionKeyExtractorClass();
			this.partitionKeyExpression = properties.getPartitionKeyExpression();
			this.partitionSelectorClass = properties.getPartitionSelectorClass();
			this.partitionSelectorExpression = properties.getPartitionSelectorExpression();
		}

		public boolean isPartitionedModule() {
			return StringUtils.hasText(this.partitionKeyExtractorClass) || this.partitionKeyExpression != null;
		}

		public int getPartitionCount() {
			return this.partitionCount;
		}

	}

}
