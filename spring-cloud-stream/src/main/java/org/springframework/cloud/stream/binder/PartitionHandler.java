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

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.expression.EvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

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

	private final ConfigurableListableBeanFactory beanFactory;

	private final EvaluationContext evaluationContext;

	private final PartitionSelectorStrategy partitionSelector;

	private final ProducerProperties producerProperties;


	/**
	 * Construct a {@code PartitionHandler}.
	 *
	 * @param beanFactory bean factory for binder
	 * @param evaluationContext evaluation context for binder
	 * @param partitionSelector configured partition selector; may be {@code null}
	 * @param properties binder properties
	 */
	public PartitionHandler(ConfigurableListableBeanFactory beanFactory,
							EvaluationContext evaluationContext,
							PartitionSelectorStrategy partitionSelector,
							ProducerProperties properties) {
		Assert.notNull(beanFactory, "BeanFactory must not be null");
		this.beanFactory = beanFactory;
		this.evaluationContext = evaluationContext;
		this.partitionSelector = partitionSelector == null
				? new DefaultPartitionSelector()
				: partitionSelector;
		this.producerProperties = properties;
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
		Object key = extractKey(message);

		int partition;
		if (this.producerProperties.getPartitionSelectorClass() != null) {
			partition = invokePartitionSelector(key);
		}
		else if (this.producerProperties.getPartitionSelectorExpression() != null) {
			partition = this.producerProperties.getPartitionSelectorExpression().getValue(
					this.evaluationContext, key, Integer.class);
		}
		else {
			partition = this.partitionSelector.selectPartition(key, producerProperties.getPartitionCount());
		}
		// protection in case a user selector returns a negative.
		return Math.abs(partition % producerProperties.getPartitionCount());
	}

	private Object extractKey(Message<?> message) {
		Object key = null;
		if (this.producerProperties.getPartitionKeyExtractorClass() != null) {
			key = invokeKeyExtractor(message);
		}
		else if (this.producerProperties.getPartitionKeyExpression() != null) {
			key = this.producerProperties.getPartitionKeyExpression().getValue(this.evaluationContext, message);
		}
		Assert.notNull(key, "Partition key cannot be null");

		return key;
	}

	private Object invokeKeyExtractor(Message<?> message) {
		PartitionKeyExtractorStrategy strategy = getBean(
				producerProperties.getPartitionKeyExtractorClass().getName(),
				PartitionKeyExtractorStrategy.class);
		return strategy.extractKey(message);
	}

	private int invokePartitionSelector(Object key) {
		PartitionSelectorStrategy strategy = getBean(
				producerProperties.getPartitionSelectorClass().getName(),
				PartitionSelectorStrategy.class);
		return strategy.selectPartition(key, producerProperties.getPartitionCount());
	}

	private <T> T getBean(String className, Class<T> type) {
		if (this.beanFactory.containsBean(className)) {
			return this.beanFactory.getBean(className, type);
		}
		else {
			synchronized (this) {
				if (this.beanFactory.containsBean(className)) {
					return this.beanFactory.getBean(className, type);
				}
				Class<?> clazz;
				try {
					clazz = ClassUtils.forName(className, this.beanFactory.getBeanClassLoader());
				}
				catch (Exception e) {
					throw new BinderException("Failed to load class: " + className, e);
				}
				try {
					@SuppressWarnings("unchecked")
					T object = (T) clazz.newInstance();
					Assert.isInstanceOf(type, object);
					this.beanFactory.registerSingleton(className, object);
					this.beanFactory.initializeBean(object, className);
					return object;
				}
				catch (Exception e) {
					throw new BinderException("Failed to instantiate class: " + className, e);
				}
			}
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

}
