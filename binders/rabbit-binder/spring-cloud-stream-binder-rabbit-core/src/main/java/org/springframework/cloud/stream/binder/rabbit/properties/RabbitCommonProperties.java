/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.properties;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.validator.constraints.Range;

import org.springframework.amqp.core.ExchangeTypes;

/**
 * @author Gary Russell
 * @author Soby Chacko
 * @since 1.2
 *
 */
public abstract class RabbitCommonProperties {

	/**
	 * DLQ name prefix.
	 */
	public static final String DEAD_LETTER_EXCHANGE = "DLX";

	/**
	 * type of exchange to declare (if necessary, and declareExchange is true).
	 */
	private String exchangeType = ExchangeTypes.TOPIC;

	/**
	 * whether to declare the exchange.
	 */
	private boolean declareExchange = true;

	/**
	 * whether to declare the exchange as durable.
	 */
	private boolean exchangeDurable = true;

	/**
	 * whether to declare the exchange as auto-delete.
	 */
	private boolean exchangeAutoDelete = false;

	/**
	 * whether a delayed message exchange should be used.
	 */
	private boolean delayedExchange = false;

	/**
	 * set to true to name the queue with only the group; default is destination.group.
	 */
	private boolean queueNameGroupOnly = false;

	/**
	 * whether to bind a queue (or queues when partitioned) to the exchange.
	 */
	private boolean bindQueue = true;

	/**
	 * routing key to bind (default # for non-partitioned, destination-instanceIndex for
	 * partitioned).
	 */
	private String bindingRoutingKey;

	/**
	 * when not null, treat 'bindingRoutingKey' as a delimited list of keys to bind.
	 */
	private String bindingRoutingKeyDelimiter;

	/**
	 * default time to live to apply to the queue when declared (ms).
	 */
	private Integer ttl;

	/**
	 * how long before an unused queue is deleted (ms).
	 */
	private Integer expires;

	/**
	 * maximum number of messages in the queue.
	 */
	private Integer maxLength;

	/**
	 * maximum number of total bytes in the queue from all messages.
	 */
	private Integer maxLengthBytes;

	/**
	 * maximum priority of messages in the queue (0-255).
	 */
	private Integer maxPriority;

	/**
	 * name of the DLQ - default is prefix+destination.dlq.
	 */
	private String deadLetterQueueName;

	/**
	 * a DLX to assign to the queue; if autoBindDlq is true, defaults to 'prefix+DLX'.
	 */
	private String deadLetterExchange;

	/**
	 * the type of the DLX, if autoBindDlq is true.
	 */
	private String deadLetterExchangeType = ExchangeTypes.DIRECT;

	/**
	 * whether to declare the dead-letter exchange when autoBindDlq is true.
	 */
	private boolean declareDlx = true;

	/**
	 * a dead letter routing key to assign to that queue; if autoBindDlq is true, defaults
	 * to destination.
	 */
	private String deadLetterRoutingKey;

	/**
	 * default time to live to apply to the dead letter queue when declared (ms).
	 */
	private Integer dlqTtl;

	/**
	 * how long before an unused dead letter queue is deleted (ms).
	 */
	private Integer dlqExpires;

	/**
	 * maximum number of messages in the dead letter queue.
	 */
	private Integer dlqMaxLength;

	/**
	 * maximum number of total bytes in the dead letter queue from all messages.
	 */
	private Integer dlqMaxLengthBytes;

	/**
	 * maximum priority of messages in the dead letter queue (0-255).
	 */
	private Integer dlqMaxPriority;

	/**
	 * if a DLQ is declared, a DLX to assign to that queue; default none.
	 */
	private String dlqDeadLetterExchange;

	/**
	 * if a DLQ is declared, a dead letter routing key to assign to that queue; default
	 * none.
	 */
	private String dlqDeadLetterRoutingKey;

	/**
	 * true to automatically bind a dead letter queue to a DLX.
	 */
	private boolean autoBindDlq;

	/**
	 * prefix for elements declared in RabbitMQ (exchanges, queues).
	 */
	private String prefix = "";

	/**
	 * true if the queue is provisioned as a lazy queue.
	 */
	private boolean lazy;

	/**
	 * true if the DLQ is provisioned as a lazy queue.
	 */
	private boolean dlqLazy;

	/**
	 * action when maxLength or maxLengthBytes is exceeded.
	 */
	private String overflowBehavior;

	/**
	 * action when maxLength or maxLengthBytes is exceeded.
	 */
	private String dlqOverflowBehavior;

	/**
	 * A map of binding arguments to apply when binding the queue to the exchange.
	 * Useful for a headers exchange, for example.
	 */
	private Map<String, String>  queueBindingArguments = new HashMap<>();

	/**
	 * A map of binding arguments to apply when binding the dlq to the exchange.
	 * Useful for a headers exchange, for example.
	 */
	private Map<String, String>  dlqBindingArguments = new HashMap<>();

	/**
	 * Configure the queue to be type quorum instead of classic.
	 */
	private QuorumConfig quorum = new QuorumConfig();

	/**
	 * Configure the DLQ to be type quorum instead of classic.
	 */
	private QuorumConfig dlqQuorum = new QuorumConfig();

	/**
	 * When true, set the 'x-single-active-consumer' queue argument to true.
	 */
	private boolean singleActiveConsumer;

	/**
	 * When true, set the 'x-single-active-consumer' queue argument to true.
	 */
	private boolean dlqSingleActiveConsumer;

	/**
	 * The bean name of a stream message converter to convert from a Spring AMQP Message
	 * to a Stream Message.
	 * @since 3.2
	 */
	private String streamStreamMessageConverterBeanName;

	public String getExchangeType() {
		return this.exchangeType;
	}

	public void setExchangeType(String exchangeType) {
		this.exchangeType = exchangeType;
	}

	public boolean isDeclareExchange() {
		return this.declareExchange;
	}

	public void setDeclareExchange(boolean declareExchange) {
		this.declareExchange = declareExchange;
	}

	public boolean isExchangeDurable() {
		return this.exchangeDurable;
	}

	public void setExchangeDurable(boolean exchangeDurable) {
		this.exchangeDurable = exchangeDurable;
	}

	public boolean isExchangeAutoDelete() {
		return this.exchangeAutoDelete;
	}

	public void setExchangeAutoDelete(boolean exchangeAutoDelete) {
		this.exchangeAutoDelete = exchangeAutoDelete;
	}

	public boolean isDelayedExchange() {
		return this.delayedExchange;
	}

	public void setDelayedExchange(boolean delayedExchange) {
		this.delayedExchange = delayedExchange;
	}

	public boolean isQueueNameGroupOnly() {
		return this.queueNameGroupOnly;
	}

	public void setQueueNameGroupOnly(boolean queueNameGroupOnly) {
		this.queueNameGroupOnly = queueNameGroupOnly;
	}

	public boolean isBindQueue() {
		return this.bindQueue;
	}

	public void setBindQueue(boolean bindQueue) {
		this.bindQueue = bindQueue;
	}

	public String getBindingRoutingKey() {
		return this.bindingRoutingKey;
	}

	public void setBindingRoutingKey(String routingKey) {
		this.bindingRoutingKey = routingKey;
	}

	public String getBindingRoutingKeyDelimiter() {
		return this.bindingRoutingKeyDelimiter;
	}

	public void setBindingRoutingKeyDelimiter(String bindingRoutingKeyDelimiter) {
		this.bindingRoutingKeyDelimiter = bindingRoutingKeyDelimiter;
	}

	public Integer getTtl() {
		return this.ttl;
	}

	public void setTtl(Integer ttl) {
		this.ttl = ttl;
	}

	public Integer getExpires() {
		return this.expires;
	}

	public void setExpires(Integer expires) {
		this.expires = expires;
	}

	public Integer getMaxLength() {
		return this.maxLength;
	}

	public void setMaxLength(Integer maxLength) {
		this.maxLength = maxLength;
	}

	public Integer getMaxLengthBytes() {
		return this.maxLengthBytes;
	}

	public void setMaxLengthBytes(Integer maxLengthBytes) {
		this.maxLengthBytes = maxLengthBytes;
	}

	@Range(min = 0, max = 255)
	public Integer getMaxPriority() {
		return this.maxPriority;
	}

	public void setMaxPriority(Integer maxPriority) {
		this.maxPriority = maxPriority;
	}

	public String getDeadLetterQueueName() {
		return this.deadLetterQueueName;
	}

	public void setDeadLetterQueueName(String deadLetterQueueName) {
		this.deadLetterQueueName = deadLetterQueueName;
	}

	public String getDeadLetterExchange() {
		return this.deadLetterExchange;
	}

	public void setDeadLetterExchange(String deadLetterExchange) {
		this.deadLetterExchange = deadLetterExchange;
	}

	public String getDeadLetterExchangeType() {
		return this.deadLetterExchangeType;
	}

	public void setDeadLetterExchangeType(String deadLetterExchangeType) {
		this.deadLetterExchangeType = deadLetterExchangeType;
	}

	public boolean isDeclareDlx() {
		return this.declareDlx;
	}

	public void setDeclareDlx(boolean declareDlx) {
		this.declareDlx = declareDlx;
	}

	public String getDeadLetterRoutingKey() {
		return this.deadLetterRoutingKey;
	}

	public void setDeadLetterRoutingKey(String deadLetterRoutingKey) {
		this.deadLetterRoutingKey = deadLetterRoutingKey;
	}

	public Integer getDlqTtl() {
		return this.dlqTtl;
	}

	public void setDlqTtl(Integer dlqTtl) {
		this.dlqTtl = dlqTtl;
	}

	public Integer getDlqExpires() {
		return this.dlqExpires;
	}

	public void setDlqExpires(Integer dlqExpires) {
		this.dlqExpires = dlqExpires;
	}

	public Integer getDlqMaxLength() {
		return this.dlqMaxLength;
	}

	public void setDlqMaxLength(Integer dlqMaxLength) {
		this.dlqMaxLength = dlqMaxLength;
	}

	public Integer getDlqMaxLengthBytes() {
		return this.dlqMaxLengthBytes;
	}

	public void setDlqMaxLengthBytes(Integer dlqMaxLengthBytes) {
		this.dlqMaxLengthBytes = dlqMaxLengthBytes;
	}

	public Integer getDlqMaxPriority() {
		return this.dlqMaxPriority;
	}

	public void setDlqMaxPriority(Integer dlqMaxPriority) {
		this.dlqMaxPriority = dlqMaxPriority;
	}

	public String getDlqDeadLetterExchange() {
		return this.dlqDeadLetterExchange;
	}

	public void setDlqDeadLetterExchange(String dlqDeadLetterExchange) {
		this.dlqDeadLetterExchange = dlqDeadLetterExchange;
	}

	public String getDlqDeadLetterRoutingKey() {
		return this.dlqDeadLetterRoutingKey;
	}

	public void setDlqDeadLetterRoutingKey(String dlqDeadLetterRoutingKey) {
		this.dlqDeadLetterRoutingKey = dlqDeadLetterRoutingKey;
	}

	public boolean isAutoBindDlq() {
		return autoBindDlq;
	}

	public void setAutoBindDlq(boolean autoBindDlq) {
		this.autoBindDlq = autoBindDlq;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public boolean isLazy() {
		return this.lazy;
	}

	public void setLazy(boolean lazy) {
		this.lazy = lazy;
	}

	public boolean isDlqLazy() {
		return this.dlqLazy;
	}

	public void setDlqLazy(boolean dlqLazy) {
		this.dlqLazy = dlqLazy;
	}

	public String getOverflowBehavior() {
		return this.overflowBehavior;
	}

	public void setOverflowBehavior(String overflowBehavior) {
		this.overflowBehavior = overflowBehavior;
	}

	public String getDlqOverflowBehavior() {
		return this.dlqOverflowBehavior;
	}

	public void setDlqOverflowBehavior(String dlqOverflowBehavior) {
		this.dlqOverflowBehavior = dlqOverflowBehavior;
	}

	public Map<String, String> getQueueBindingArguments() {
		return this.queueBindingArguments;
	}

	public void setQueueBindingArguments(Map<String, String> queueBindingArguments) {
		this.queueBindingArguments = queueBindingArguments;
	}

	public Map<String, String> getDlqBindingArguments() {
		return this.dlqBindingArguments;
	}

	public void setDlqBindingArguments(Map<String, String> dlqBindingArguments) {
		this.dlqBindingArguments = dlqBindingArguments;
	}

	public QuorumConfig getQuorum() {
		return this.quorum;
	}

	public void setQuorum(QuorumConfig quorum) {
		this.quorum = quorum;
	}

	public QuorumConfig getDlqQuorum() {
		return this.dlqQuorum;
	}

	public void setDlqQuorum(QuorumConfig dlqQuorum) {
		this.dlqQuorum = dlqQuorum;
	}

	public boolean isSingleActiveConsumer() {
		return this.singleActiveConsumer;
	}

	public void setSingleActiveConsumer(boolean singleActiveConsumer) {
		this.singleActiveConsumer = singleActiveConsumer;
	}

	public boolean isDlqSingleActiveConsumer() {
		return this.dlqSingleActiveConsumer;
	}

	public void setDlqSingleActiveConsumer(boolean dlqSingleActiveConsumer) {
		this.dlqSingleActiveConsumer = dlqSingleActiveConsumer;
	}

	public String getStreamStreamMessageConverterBeanName() {
		return this.streamStreamMessageConverterBeanName;
	}

	public void setStreamStreamMessageConverterBeanName(String streamStreamMessageConverterBeanName) {
		this.streamStreamMessageConverterBeanName = streamStreamMessageConverterBeanName;
	}

	public static class QuorumConfig {

		private boolean enabled;

		private Integer initialGroupSize;

		private Integer deliveryLimit;

		public boolean isEnabled() {
			return this.enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public Integer getInitialGroupSize() {
			return this.initialGroupSize;
		}

		public void setInitialGroupSize(Integer initialGroupSize) {
			this.initialGroupSize = initialGroupSize;
		}

		public Integer getDeliveryLimit() {
			return this.deliveryLimit;
		}

		public void setDeliveryLimit(Integer deliveryLimit) {
			this.deliveryLimit = deliveryLimit;
		}

	}

}
