/*
 * Copyright 2016-2019 the original author or authors.
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.Min;

import org.springframework.messaging.Message;

/**
 * Common consumer properties - spring.cloud.stream.bindings.[destinationName].consumer.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Soby Chacko
 * @author Oleg Zhurakousky
 * @author Nicolas Homble
 * @author Michael Michailidis
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ConsumerProperties {

	/**
	 * Signals if this consumer needs to be started automatically.
	 *
	 * Default: true
	 */
	private boolean autoStartup = true;

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
	 * count of this consumer (if different from spring.cloud.stream.instanceCount). When
	 * set to a negative value, it will default to spring.cloud.stream.instanceCount. See
	 * that property for more information. Default: -1 NOTE: This setting will override
	 * the one set in 'spring.cloud.stream.instance-count'
	 */
	private int instanceCount = -1;

	/**
	 * When set to a value greater than equal to zero, allows customizing the instance
	 * index of this consumer (if different from spring.cloud.stream.instanceIndex). When
	 * set to a negative value, it will default to spring.cloud.stream.instanceIndex. See
	 * that property for more information. Default: -1 NOTE: This setting will override
	 * the one set in 'spring.cloud.stream.instance-index'
	 */
	private int instanceIndex = -1;

	/**
	 * When set it will allow the customization of the consumer to spawn a consumer for
	 * each item in the list. All negative indexes will be discarded. Default: null
	 * NOTE: This setting will disable the instance-index
	 */
	private List<Integer> instanceIndexList;

	/**
	 * The number of attempts to process the message (including the first) in the event of
	 * processing failures. This is a RetryTemplate configuration which is provided by the
	 * framework. Default: 3. Set to 1 to disable retry. You can also provide custom
	 * RetryTemplate in the event you want to take complete control of the RetryTemplate.
	 * Simply configure it as @Bean inside your application configuration.
	 */
	private int maxAttempts = 3;

	/**
	 * The backoff initial interval on retry. This is a RetryTemplate configuration which
	 * is provided by the framework. Default: 1000 ms. You can also provide custom
	 * RetryTemplate in the event you want to take complete control of the RetryTemplate.
	 * Simply configure it as @Bean inside your application configuration.
	 */
	private int backOffInitialInterval = 1000;

	/**
	 * The maximum backoff interval. This is a RetryTemplate configuration which is
	 * provided by the framework. Default: 10000 ms. You can also provide custom
	 * RetryTemplate in the event you want to take complete control of the RetryTemplate.
	 * Simply configure it as @Bean inside your application configuration.
	 */
	private int backOffMaxInterval = 10000;

	/**
	 * The backoff multiplier.This is a RetryTemplate configuration which is provided by
	 * the framework. Default: 2.0. You can also provide custom RetryTemplate in the event
	 * you want to take complete control of the RetryTemplate. Simply configure it
	 * as @Bean inside your application configuration.
	 */
	private double backOffMultiplier = 2.0;

	/**
	 * Whether exceptions thrown by the listener that are not listed in the
	 * 'retryableExceptions' are retryable.
	 */
	private boolean defaultRetryable = true;

	/**
	 * Allows you to further qualify which RetryTemplate to use for a specific consumer
	 * binding..
	 */
	private String retryTemplateName;

	/**
	 * A map of Throwable class names in the key and a boolean in the value. Specify those
	 * exceptions (and subclasses) that will or won't be retried.
	 */
	private Map<Class<? extends Throwable>, Boolean> retryableExceptions = new LinkedHashMap<>();

	/**
	 * When set to none, disables header parsing on input. Effective only for messaging
	 * middleware that does not support message headers natively and requires header
	 * embedding. This option is useful when consuming data from non-Spring Cloud Stream
	 * applications when native headers are not supported. When set to headers, uses the
	 * middleware’s native header mechanism. When set to 'embeddedHeaders', embeds headers
	 * into the message payload. Default: depends on binder implementation. Rabbit and
	 * Kafka binders currently distributed with spring cloud stream support headers
	 * natively.
	 */
	private HeaderMode headerMode;

	/**
	 * When set to true, the inbound message is deserialized directly by client library,
	 * which must be configured correspondingly (e.g. setting an appropriate Kafka
	 * producer value serializer). NOTE: This is binder specific setting which has no
	 * effect if binder does not support native serialization/deserialization. Currently
	 * only Kafka binder supports it. Default: 'false'
	 */
	private boolean useNativeDecoding;

	/**
	 * When set to true, the underlying binder will natively multiplex destinations on the
	 * same input binding. For example, in the case of a comma separated multiple
	 * destinations, the core framework will skip binding them individually if this is set
	 * to true, but delegate that responsibility to the binder.
	 *
	 * By default this property is set to `false` and the binder will individually bind
	 * each destinations in case of a comma separated multi destination list. The
	 * individual binder implementations that need to support multiple input bindings
	 * natively (multiplex) can enable this property.
	 */
	private boolean multiplex;

	/**
	 * When set to true, if the binder supports it, the messages emitted will have a {@link List}
	 * payload; When used in conjunction with functions, the function can receive a list of
	 * objects (or {@link Message}s) with the payloads converted if necessary.
	 *
	 * @since 3.0
	 */
	private boolean batchMode;

	public String getRetryTemplateName() {
		return retryTemplateName;
	}

	public void setRetryTemplateName(String retryTemplateName) {
		this.retryTemplateName = retryTemplateName;
	}

	@Min(value = 1, message = "Concurrency should be greater than zero.")
	public int getConcurrency() {
		return this.concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public boolean isPartitioned() {
		return this.partitioned;
	}

	public void setPartitioned(boolean partitioned) {
		this.partitioned = partitioned;
	}

	@Min(value = -1, message = "Instance count should be greater than or equal to -1.")
	public int getInstanceCount() {
		return this.instanceCount;
	}

	public void setInstanceCount(int instanceCount) {
		this.instanceCount = instanceCount;
	}

	@Min(value = -1, message = "Instance index should be greater than or equal to -1")
	public int getInstanceIndex() {
		return this.instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	public List<Integer> getInstanceIndexList() {
		return this.instanceIndexList;
	}

	public void setInstanceIndexList(List<Integer> instanceIndexList) {
		this.instanceIndexList = instanceIndexList;
	}

	@Min(value = 1, message = "Max attempts should be greater than zero.")
	public int getMaxAttempts() {
		return this.maxAttempts;
	}

	public void setMaxAttempts(int maxAttempts) {
		this.maxAttempts = maxAttempts;
	}

	@Min(value = 1, message = "Backoff initial interval should be greater than zero.")
	public int getBackOffInitialInterval() {
		return this.backOffInitialInterval;
	}

	public void setBackOffInitialInterval(int backOffInitialInterval) {
		this.backOffInitialInterval = backOffInitialInterval;
	}

	@Min(value = 1, message = "Backoff max interval should be greater than zero.")
	public int getBackOffMaxInterval() {
		return this.backOffMaxInterval;
	}

	public void setBackOffMaxInterval(int backOffMaxInterval) {
		this.backOffMaxInterval = backOffMaxInterval;
	}

	@Min(value = 1, message = "Backoff multiplier should be greater than zero.")
	public double getBackOffMultiplier() {
		return this.backOffMultiplier;
	}

	public void setBackOffMultiplier(double backOffMultiplier) {
		this.backOffMultiplier = backOffMultiplier;
	}

	public boolean isDefaultRetryable() {
		return this.defaultRetryable;
	}

	public void setDefaultRetryable(boolean defaultRetryable) {
		this.defaultRetryable = defaultRetryable;
	}

	public Map<Class<? extends Throwable>, Boolean> getRetryableExceptions() {
		return this.retryableExceptions;
	}

	public void setRetryableExceptions(
			Map<Class<? extends Throwable>, Boolean> retryableExceptions) {
		this.retryableExceptions = retryableExceptions;
	}

	public HeaderMode getHeaderMode() {
		return this.headerMode;
	}

	public void setHeaderMode(HeaderMode headerMode) {
		this.headerMode = headerMode;
	}

	public boolean isUseNativeDecoding() {
		return this.useNativeDecoding;
	}

	public void setUseNativeDecoding(boolean useNativeDecoding) {
		this.useNativeDecoding = useNativeDecoding;
	}

	public boolean isMultiplex() {
		return this.multiplex;
	}

	public void setMultiplex(boolean multiplex) {
		this.multiplex = multiplex;
	}

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public boolean isBatchMode() {
		return this.batchMode;
	}

	public void setBatchMode(boolean batchMode) {
		this.batchMode = batchMode;
	}

}
