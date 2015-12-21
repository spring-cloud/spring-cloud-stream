/*
 * Copyright 2014-2016 the original author or authors.
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

import java.util.Properties;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringUtils;


/**
 * Base class for binding-specific property accessors; common properties
 * are defined here.
 *
 * @author Gary Russell
 */
public abstract class AbstractBindingPropertiesAccessor {

	private static final SpelExpressionParser spelExpressionParser = new SpelExpressionParser();

	private final Properties properties;

	public AbstractBindingPropertiesAccessor(Properties properties) {
		if (properties == null) {
			this.properties = new Properties();
		}
		else {
			this.properties = properties;
		}
	}

	/**
	 * Return the underlying properties object.
	 * @return The properties.
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * Return the property for the key, or null if it doesn't exist.
	 * @param key The property.
	 * @return The key.
	 */
	public String getProperty(String key) {
		return this.properties.getProperty(key);
	}

	/**
	 * Return the property for the key, or the default value if the
	 * property doesn't exist.
	 * @param key The key.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public String getProperty(String key, String defaultValue) {
		return this.properties.getProperty(key, defaultValue);
	}

	/**
	 * Return the property for the key, or the default value if the
	 * property doesn't exist.
	 * @param key The key.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public boolean getProperty(String key, boolean defaultValue) {
		String property = this.properties.getProperty(key);
		if (property != null) {
			return Boolean.parseBoolean(property);
		}
		else {
			return defaultValue;
		}
	}

	/**
	 * Return the property for the key, or the default value if the
	 * property doesn't exist.
	 * @param key The key.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public int getProperty(String key, int defaultValue) {
		String property = this.properties.getProperty(key);
		if (property != null) {
			return Integer.parseInt(property);
		}
		else {
			return defaultValue;
		}
	}

	/**
	 * Return the property for the key, or the default value if the
	 * property doesn't exist.
	 * @param key The key.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public long getProperty(String key, long defaultValue) {
		String property = this.properties.getProperty(key);
		if (property != null) {
			return Long.parseLong(property);
		}
		else {
			return defaultValue;
		}
	}

	/**
	 * Return the property for the key, or the default value if the
	 * property doesn't exist.
	 * @param key The key.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public double getProperty(String key, double defaultValue) {
		String property = properties.getProperty(key);
		if (property != null) {
			return Double.parseDouble(property);
		}
		else {
			return defaultValue;
		}
	}

	/**
	 * Return the 'concurrency' property or the default value.
	 * The meaning of concurrency depends on the binder implementation.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public int getConcurrency(int defaultValue) {
		return getProperty(BinderPropertyKeys.CONCURRENCY, defaultValue);
	}

	/**
	 * Return the 'maxConcurrency' property or the default value.
	 * The meaning of maxConcurrency depends on the binder implementation.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public int getMaxConcurrency(int defaultValue) {
		return getProperty(BinderPropertyKeys.MAX_CONCURRENCY, defaultValue);
	}

	// Retry properties

	/**
	 * Return the 'maxAttempts' property or the default value.
	 * This is used in the retry template's SimpleRetryPolicy
	 * in binders that support retry.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public int getMaxAttempts(int defaultValue) {
		return getProperty(BinderPropertyKeys.MAX_ATTEMPTS, defaultValue);
	}

	/**
	 * Return the 'backOffInitialInterval' property or the default value.
	 * This is used in the retry template's ExponentialBackOffPolicy
	 * in binders that support retry.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public long getBackOffInitialInterval(long defaultValue) {
		return getProperty(BinderPropertyKeys.BACK_OFF_INITIAL_INTERVAL, defaultValue);
	}

	/**
	 * Return the 'backOffMultiplier' property or the default value.
	 * This is used in the retry template's ExponentialBackOffPolicy
	 * in binders that support retry.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public double getBackOffMultiplier(double defaultValue) {
		return getProperty(BinderPropertyKeys.BACK_OFF_MULTIPLIER, defaultValue);
	}

	/**
	 * Return the 'backOffMaxInterval' property or the default value.
	 * This is used in the retry template's ExponentialBackOffPolicy
	 * in binders that support retry.
	 * @param defaultValue The default value.
	 * @return The property or default value.
	 */
	public long getBackOffMaxInterval(long defaultValue) {
		return getProperty(BinderPropertyKeys.BACK_OFF_MAX_INTERVAL, defaultValue);
	}

	// Partitioning

	/**
	 * A class name for extracting partition keys from messages.
	 * @return The class name,
	 */
	public String getPartitionKeyExtractorClass() {
		return getProperty(BinderPropertyKeys.PARTITION_KEY_EXTRACTOR_CLASS);
	}

	/**
	 * The expression to determine the partition key, evaluated against the
	 * message as the root object.
	 * @return The key.
	 */
	public Expression getPartitionKeyExpression() {
		String partionKeyExpression = getProperty(BinderPropertyKeys.PARTITION_KEY_EXPRESSION);
		Expression expression = null;
		if (partionKeyExpression != null) {
			expression = spelExpressionParser.parseExpression(partionKeyExpression);
		}
		return expression;
	}

	/**
	 * A class name for calculating a partition from a key.
	 * @return The class name,
	 */
	public String getPartitionSelectorClass() {
		return getProperty(BinderPropertyKeys.PARTITION_SELECTOR_CLASS);
	}

	/**
	 * The expression evaluated against the partition key to determine
	 * the partition to which the message will be sent. The result should
	 * be an integer that will subsequently be mod'd with the module's
	 * partition count.
	 * @return The expression.
	 */
	public Expression getPartitionSelectorExpression() {
		String partionSelectorExpression = getProperty(BinderPropertyKeys.PARTITION_SELECTOR_EXPRESSION);
		Expression expression = null;
		if (partionSelectorExpression != null) {
			expression = spelExpressionParser.parseExpression(partionSelectorExpression);
		}
		return expression;
	}

	/**
	 * The sequence number for this module.
	 *
	 * @return the sequence number.
	 */
	public int getSequence() {
		return getProperty(BinderPropertyKeys.SEQUENCE, 1);
	}

	/**
	 * The module count.
	 *
	 * @return the module count.
	 */
	public int getCount() {
		return getProperty(BinderPropertyKeys.COUNT, 1);
	}

	/**
	 * The next module count for non-sink modules
	 * @return the next module count
	 */
	public int getNextModuleCount() {
		return getProperty(BinderPropertyKeys.NEXT_MODULE_COUNT, 1);
	}

	/**
	 * The partition index that this consumer supports.
	 * @return The partition index.
	 */
	public int getPartitionIndex() {
		return getProperty(BinderPropertyKeys.PARTITION_INDEX, -1);
	}

	// Batching

	/**
	 * If true, enable batching.
	 * @param defaultValue the default value.
	 * @return the property or default value.
	 */
	public boolean isBatchingEnabled(boolean defaultValue) {
		return getProperty(BinderPropertyKeys.BATCHING_ENABLED, defaultValue);
	}

	/**
	 * The batch size.
	 * @param defaultValue the default value.
	 * @return the property or default value.
	 */
	public int getBatchSize(int defaultValue) {
		return getProperty(BinderPropertyKeys.BATCH_SIZE, defaultValue);
	}

	/**
	 * The batch buffer limit.
	 * @param defaultValue the default value.
	 * @return the property or default value.
	 */
	public int geteBatchBufferLimit(int defaultValue) {
		return getProperty(BinderPropertyKeys.BATCH_BUFFER_LIMIT, defaultValue);
	}

	/**
	 * The batch timeout.
	 * @param defaultValue the default value.
	 * @return the property or default value.
	 */
	public long getBatchTimeout(long defaultValue) {
		return getProperty(BinderPropertyKeys.BATCH_TIMEOUT, defaultValue);
	}

	/**
	 * If true, messages will be compressed.
	 * @param defaultValue the default value.
	 * @return the property or default value.
	 */
	public boolean isCompress(boolean defaultValue) {
		return getProperty(BinderPropertyKeys.COMPRESS, defaultValue);
	}

	/**
	 * If true, subscriptions to taps/topics will be durable.
	 * @param defaultValue the default value.
	 * @return the property or default value.
	 */
	public boolean isDurable(boolean defaultValue) {
		return getProperty(BinderPropertyKeys.DURABLE, defaultValue);
	}

	// Utility methods

	/**
	 * Convert a comma-delimited String property to a String[] if
	 * present, or return the default value.
	 * @param value The property value.
	 * @param defaultValue The default value.
	 * @return The converted property or default value.
	 */
	protected String[] asStringArray(String value, String[] defaultValue) {
		if (StringUtils.hasText(value)) {
			return StringUtils.commaDelimitedListToStringArray(value);
		}
		else {
			return defaultValue;
		}
	}

	@Override
	public String toString() {
		return this.properties.toString();
	}

}
