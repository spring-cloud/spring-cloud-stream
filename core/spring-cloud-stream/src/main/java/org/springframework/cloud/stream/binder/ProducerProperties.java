/*
 * Copyright 2016-present the original author or authors.
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

import java.io.IOException;
import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.springframework.expression.Expression;

/**
 * Common producer properties.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Oleg Zhurakousky
 */
@JsonInclude(Include.NON_DEFAULT)
public class ProducerProperties {

	/**
	 * Binding name for this producer binding.
	 */
	private String bindingName;

	/**
	 * Signals if this producer needs to be started automatically. Default: true
	 */
	private boolean autoStartup = true;

	@JsonSerialize(using = ExpressionSerializer.class)
	private Expression partitionKeyExpression;

	/**
	 * The name of the bean that implements {@link PartitionKeyExtractorStrategy}\. Used
	 * to extract a key used to compute the partition id (see 'partitionSelector*') <br>
	 * Mutually exclusive with 'partitionKeyExpression'.
	 */
	private String partitionKeyExtractorName;

	/**
	 * The name of the bean that implements {@link PartitionSelectorStrategy}\. Used to
	 * determine partition id based on partition key (see 'partitionKeyExtractor*'). <br>
	 * Mutually exclusive with 'partitionSelectorExpression'.
	 */
	private String partitionSelectorName;

	@JsonSerialize(using = ExpressionSerializer.class)
	private Expression partitionSelectorExpression;

	private int partitionCount = 1;

	private String[] requiredGroups = new String[] {};

	private HeaderMode headerMode;

	private boolean useNativeEncoding = false;

	private boolean errorChannelEnabled = false;

	private PollerProperties poller;

	private boolean dynamicPartitionUpdatesEnabled = false;

	public String getBindingName() {
		return bindingName;
	}

	/**
	 * This method is not intended as a configuration property to set by the applications.
	 * Therefore, we are not providing a proper setter method for this.
	 * @param bindingName binding name populated by the framework.
	 */
	public void populateBindingName(String bindingName) {
		this.bindingName = bindingName;
	}

	public Expression getPartitionKeyExpression() {
		return this.partitionKeyExpression;
	}

	public void setPartitionKeyExpression(Expression partitionKeyExpression) {
		this.partitionKeyExpression = partitionKeyExpression;
	}

	public boolean isPartitioned() {
		return this.partitionKeyExpression != null
				|| this.partitionKeyExtractorName != null;
	}

	public Expression getPartitionSelectorExpression() {
		return this.partitionSelectorExpression;
	}

	public void setPartitionSelectorExpression(Expression partitionSelectorExpression) {
		this.partitionSelectorExpression = partitionSelectorExpression;
	}

	public int getPartitionCount() {
		return this.partitionCount;
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}

	public String[] getRequiredGroups() {
		return this.requiredGroups;
	}

	public void setRequiredGroups(String... requiredGroups) {
		this.requiredGroups = requiredGroups;
	}

	public boolean isValidPartitionKeyProperty() {
		return this.partitionKeyExpression == null;
	}

	public boolean isValidPartitionSelectorProperty() {
		return this.partitionSelectorExpression == null;
	}

	public HeaderMode getHeaderMode() {
		return this.headerMode;
	}

	public void setHeaderMode(HeaderMode headerMode) {
		this.headerMode = headerMode;
	}

	public boolean isUseNativeEncoding() {
		return this.useNativeEncoding;
	}

	public void setUseNativeEncoding(boolean useNativeEncoding) {
		this.useNativeEncoding = useNativeEncoding;
	}

	public boolean isErrorChannelEnabled() {
		return this.errorChannelEnabled;
	}

	public void setErrorChannelEnabled(boolean errorChannelEnabled) {
		this.errorChannelEnabled = errorChannelEnabled;
	}

	public String getPartitionKeyExtractorName() {
		return this.partitionKeyExtractorName;
	}

	public void setPartitionKeyExtractorName(String partitionKeyExtractorName) {
		this.partitionKeyExtractorName = partitionKeyExtractorName;
	}

	public String getPartitionSelectorName() {
		return this.partitionSelectorName;
	}

	public void setPartitionSelectorName(String partitionSelectorName) {
		this.partitionSelectorName = partitionSelectorName;
	}

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public PollerProperties getPoller() {
		return poller;
	}

	public void setPoller(PollerProperties poller) {
		this.poller = poller;
	}

	/**
	 * Returns status of property dynamicPartitionUpdatesEnabled.
	 * @return true if dynamic updates should are enabled otherwise false
	 */
	public boolean isDynamicPartitionUpdatesEnabled() {
		return dynamicPartitionUpdatesEnabled;
	}

	/**
	 * A flag which enables/disables partition count updates during runtime. Disabled by default.
	 * Depends on binder if supported or not.
	 * Currently only supported by kafka binder (see 'Partitioning with the Kafka Binder' documentation for details)
	 * @param enabled true if dynamic updates should be enabled otherwise false
	 */
	public void setDynamicPartitionUpdatesEnabled(boolean enabled) {
		this.dynamicPartitionUpdatesEnabled = enabled;
	}

	static class ExpressionSerializer extends JsonSerializer<Expression> {

		@Override
		public void serialize(Expression expression, JsonGenerator jsonGenerator,
				SerializerProvider serializerProvider) throws IOException {
			if (expression != null) {
				jsonGenerator.writeString(expression.getExpressionString());
			}
		}

	}

	public static class PollerProperties {

		private Duration fixedDelay = Duration.ofMillis(1000);

		private long maxMessagesPerPoll = 1L;

		private String cron;

		private Duration initialDelay = Duration.ofMillis(0);

		public long getMaxMessagesPerPoll() {
			return maxMessagesPerPoll;
		}

		public void setMaxMessagesPerPoll(long maxMessagesPerPoll) {
			this.maxMessagesPerPoll = maxMessagesPerPoll;
		}

		public String getCron() {
			return cron;
		}

		public void setCron(String cron) {
			this.cron = cron;
		}

		public Duration getInitialDelay() {
			return initialDelay;
		}

		public void setInitialDelay(Duration initialDelay) {
			this.initialDelay = initialDelay;
		}

		public Duration getFixedDelay() {
			return fixedDelay;
		}

		public void setFixedDelay(Duration fixedDelay) {
			this.fixedDelay = fixedDelay;
		}
	}

}
