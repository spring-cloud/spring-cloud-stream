/*
 * Copyright 2016-2020 the original author or authors.
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

import java.lang.reflect.Field;
import java.util.Map;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.expression.EvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

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
 * @author Oleg Zhurakousky
 */
public class PartitionHandler {

	private final EvaluationContext evaluationContext;

	private final ProducerProperties producerProperties;

	private final PartitionKeyExtractorStrategy partitionKeyExtractorStrategy;

	private final PartitionSelectorStrategy partitionSelectorStrategy;

	private final ConfigurableListableBeanFactory beanFactory;

	private volatile int partitionCount;

	/**
	 * Construct a {@code PartitionHandler}.
	 * @param evaluationContext evaluation context for binder
	 * @param properties binder properties
	 * @param partitionKeyExtractorStrategy PartitionKeyExtractor strategy
	 * @param partitionSelectorStrategy PartitionSelector strategy
	 *
	 * @deprecated since 3.0.2. Please use another constructor which allows you to pass an instance of beanFactory
	 */
	@Deprecated
	public PartitionHandler(EvaluationContext evaluationContext,
			ProducerProperties properties,
			PartitionKeyExtractorStrategy partitionKeyExtractorStrategy,
			PartitionSelectorStrategy partitionSelectorStrategy) {

		this(evaluationContext, properties, (ConfigurableListableBeanFactory) extractBeanFactoryFromEvaluationContext(evaluationContext));
	}

	/**
	 * Construct a {@code PartitionHandler}.
	 * @param evaluationContext evaluation context for binder
	 * @param properties binder properties
	 * @param beanFactory instance of ConfigurableListableBeanFactory
	 *
	 * @since 3.0.2
	 */
	public PartitionHandler(EvaluationContext evaluationContext,
			ProducerProperties properties, ConfigurableListableBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		this.evaluationContext = evaluationContext;
		this.producerProperties = properties;
		this.partitionKeyExtractorStrategy = this.getPartitionKeyExtractorStrategy(properties);
		this.partitionSelectorStrategy = this.getPartitionSelectorStrategy(properties);
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

	private PartitionKeyExtractorStrategy getPartitionKeyExtractorStrategy(
			ProducerProperties producerProperties) {
		PartitionKeyExtractorStrategy partitionKeyExtractor;
		if (StringUtils.hasText(producerProperties.getPartitionKeyExtractorName())) {
			partitionKeyExtractor = (PartitionKeyExtractorStrategy) this.beanFactory.getBean(
					producerProperties.getPartitionKeyExtractorName(),
					PartitionKeyExtractorStrategy.class);
			Assert.notNull(partitionKeyExtractor,
					"PartitionKeyExtractorStrategy bean with the name '"
							+ producerProperties.getPartitionKeyExtractorName()
							+ "' can not be found. Has it been configured (e.g., @Bean)?");
		}
		else {
			Map<String, PartitionKeyExtractorStrategy> extractors = this.beanFactory
					.getBeansOfType(PartitionKeyExtractorStrategy.class);
			Assert.isTrue(extractors.size() <= 1,
					"Multiple  beans of type 'PartitionKeyExtractorStrategy' found. "
							+ extractors + ". Please "
							+ "use 'spring.cloud.stream.bindings.output.producer.partitionKeyExtractorName' property to specify "
							+ "the name of the bean to be used.");
			partitionKeyExtractor = CollectionUtils.isEmpty(extractors) ? null
					: extractors.values().iterator().next();
		}
		return partitionKeyExtractor;
	}

	private PartitionSelectorStrategy getPartitionSelectorStrategy(
			ProducerProperties producerProperties) {
		PartitionSelectorStrategy partitionSelector;
		if (StringUtils.hasText(producerProperties.getPartitionSelectorName())) {
			partitionSelector = this.beanFactory.getBean(
					producerProperties.getPartitionSelectorName(),
					PartitionSelectorStrategy.class);
			Assert.notNull(partitionSelector,
					"PartitionSelectorStrategy bean with the name '"
							+ producerProperties.getPartitionSelectorName()
							+ "' can not be found. Has it been configured (e.g., @Bean)?");
		}
		else {
			Map<String, PartitionSelectorStrategy> selectors = this.beanFactory
					.getBeansOfType(PartitionSelectorStrategy.class);
			Assert.isTrue(selectors.size() <= 1,
					"Multiple  beans of type 'PartitionSelectorStrategy' found. "
							+ selectors + ". Please "
							+ "use 'spring.cloud.stream.bindings.output.producer.partitionSelectorName' property to specify "
							+ "the name of the bean to be used.");
			partitionSelector = CollectionUtils.isEmpty(selectors)
					? new DefaultPartitionSelector()
					: selectors.values().iterator().next();
		}
		return partitionSelector;
	}

	private static BeanFactory extractBeanFactoryFromEvaluationContext(EvaluationContext evaluationContext) {
		try {
			Field field = ReflectionUtils.findField(BeanFactoryResolver.class, "beanFactory");
			field.setAccessible(true);
			return (BeanFactory) field.get(evaluationContext);
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to extract beanFactory from EvaluationContext. Please use different constructor"
					+ " which allows you to pass the instance of the beanFactory.");
		}
	}

	/**
	 * Default partition strategy; only works on keys with "real" hash codes, such as
	 * String. Caller now always applies modulo so no need to do so here.
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
