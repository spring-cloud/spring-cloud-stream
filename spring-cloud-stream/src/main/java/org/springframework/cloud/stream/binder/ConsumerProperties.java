/*
 * Copyright 2016-2018 the original author or authors.
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

import javax.validation.constraints.Min;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Common consumer properties.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Soby Chacko
 * @author Oleg Zhurakousky
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ConsumerProperties {

	/**
	 * The concurrency setting of the consumer. Default: 1.
	 */
	private int concurrency = 1;

	/**
	 * Whether the consumer receives data from a partitioned producer. Default: 'false'.
	 */
	private boolean partitioned;

	/**
	 * When set to a value greater than equal to zero, allows customizing the instance
	 * count of this consumer (if different from spring.cloud.stream.instanceCount).
	 * When set to a negative value, it will default to spring.cloud.stream.instanceCount.
	 * See that property for more information.
	 * Default: -1
	 * NOTE: This setting will override the one set in 'spring.cloud.stream.instance-count'
	 */
	private int instanceCount = -1;

	/**
	 * When set to a value greater than equal to zero, allows customizing the instance
	 * index of this consumer (if different from spring.cloud.stream.instanceIndex).
	 * When set to a negative value, it will default to spring.cloud.stream.instanceIndex.
	 * See that property for more information.
	 * Default: -1
	 * NOTE: This setting will override the one set in 'spring.cloud.stream.instance-index'
	 */
	private int instanceIndex = -1;

	/**
	 * The number of attempts to process the message (including the first)
	 * in the event of processing failures. This is a  RetryTemplate configuration
	 * which is provided by the framework.
	 * Default: 3. Set to 1 to disable retry. You can also provide custom RetryTemplate
	 * in the event you want to take complete control of the RetryTemplate. Simply configure
	 * it as @Bean inside your application configuration.
	 */
	private int maxAttempts = 3;

	/**
	 * The backoff initial interval on retry. This is a  RetryTemplate configuration
	 * which is provided by the framework.
	 * Default: 1000 ms.
	 * You can also provide custom RetryTemplate
	 * in the event you want to take complete control of the RetryTemplate. Simply configure
	 * it as @Bean inside your application configuration.
	 */
	private int backOffInitialInterval = 1000;

	/**
	 * The maximum backoff interval. This is a  RetryTemplate configuration
	 * which is provided by the framework.
	 * Default: 10000 ms.
	 * You can also provide custom RetryTemplate
	 * in the event you want to take complete control of the RetryTemplate. Simply configure
	 * it as @Bean inside your application configuration.
	 */
	private int backOffMaxInterval = 10000;

	/**
	 * The backoff multiplier.This is a  RetryTemplate configuration
	 * which is provided by the framework.
	 * Default: 2.0.
	 * You can also provide custom RetryTemplate
	 * in the event you want to take complete control of the RetryTemplate. Simply configure
	 * it as @Bean inside your application configuration.
	 */
	private double backOffMultiplier = 2.0;

	/**
	 * When set to none, disables header parsing on input. Effective only
	 * for messaging middleware that does not support message headers natively
	 * and requires header embedding. This option is useful when consuming data
	 * from non-Spring Cloud Stream applications when native headers are not
	 * supported. When set to headers, uses the middleware’s native header mechanism.
	 * When set to embeddedHeaders, embeds headers into the message payload.
	 * Default: depends on binder implementation. Rabbit and Kafka binders currently
	 * distributed with spring cloud stream support headers natively.
	 */
	private HeaderMode headerMode;

	/**
	 * When set to true, the inbound message is deserialized directly by client library,
	 * which must be configured correspondingly (e.g. setting an appropriate Kafka producer value serializer).
	 * NOTE: This is binder specific setting which has no effect if binder does not support native
	 * serialization/deserialization. Currently only Kafka binder supports it.
	 * Default: 'false'
	 */
	private boolean useNativeDecoding;

	/**
	 * When set to true, the underlying binder will natively multiplex destinations on the same input binding.
	 * For example, in the case of a comma separated multiple destinations, the core framework will skip binding
	 * them individually if this is set to true, but delegate that responsibility to the binder.
	 *
	 * By default this property is set to `false` and the binder will individually bind each destinations in case
	 * of a comma separated multi destination list. The individual binder implementations that need to support multiple
	 * input bindings natively (multiplex) can enable this property. Under normal circumstances, the end users are
	 * not expected to enable or disable this property directly.
	 */
	private boolean multiplex;

	@Min(value = 1, message = "Concurrency should be greater than zero.")
	public int getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public boolean isPartitioned() {
		return partitioned;
	}

	public void setPartitioned(boolean partitioned) {
		this.partitioned = partitioned;
	}

	@Min(value = 1, message = "Instance count should be greater than zero.")
	public int getInstanceCount() {
		return instanceCount;
	}

	public void setInstanceCount(int instanceCount) {
		this.instanceCount = instanceCount;
	}

	@Min(value = 0, message = "Instance index should be greater than or equal to 0")
	public int getInstanceIndex() {
		return instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	@Min(value = 1, message = "Max attempts should be greater than zero.")
	public int getMaxAttempts() {
		return maxAttempts;
	}

	public void setMaxAttempts(int maxAttempts) {
		this.maxAttempts = maxAttempts;
	}

	@Min(value = 1, message = "Backoff initial interval should be greater than zero.")
	public int getBackOffInitialInterval() {
		return backOffInitialInterval;
	}

	public void setBackOffInitialInterval(int backOffInitialInterval) {
		this.backOffInitialInterval = backOffInitialInterval;
	}

	@Min(value = 1, message = "Backoff max interval should be greater than zero.")
	public int getBackOffMaxInterval() {
		return backOffMaxInterval;
	}

	public void setBackOffMaxInterval(int backOffMaxInterval) {
		this.backOffMaxInterval = backOffMaxInterval;
	}

	@Min(value = 1, message = "Backoff multiplier should be greater than zero.")
	public double getBackOffMultiplier() {
		return backOffMultiplier;
	}

	public void setBackOffMultiplier(double backOffMultiplier) {
		this.backOffMultiplier = backOffMultiplier;
	}

	public HeaderMode getHeaderMode() {
		return this.headerMode;
	}

	public void setHeaderMode(HeaderMode headerMode) {
		this.headerMode = headerMode;
	}

	public boolean isUseNativeDecoding() {
		return useNativeDecoding;
	}

	public void setUseNativeDecoding(boolean useNativeDecoding) {
		this.useNativeDecoding = useNativeDecoding;
	}

	public boolean isMultiplex() {
		return multiplex;
	}

	public void setMultiplex(boolean multiplex) {
		this.multiplex = multiplex;
	}
}
