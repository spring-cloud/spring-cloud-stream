/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.properties;

import javax.validation.constraints.Min;

import org.springframework.amqp.core.MessageDeliveryMode;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class RabbitProducerProperties extends RabbitCommonProperties {

	private String[] requestHeaderPatterns = new String[] {"STANDARD_REQUEST_HEADERS", "*"};

	private boolean compress;

	private boolean batchingEnabled;

	private int batchSize = 100;

	private int batchBufferLimit = 10000;

	private int batchTimeout = 5000;

	private boolean transacted;

	private MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;

	private String[] replyHeaderPatterns = new String[] {"STANDARD_REPLY_HEADERS", "*"};

	/**
	 * When using a delayed message exchange, a SpEL expression to determine the delay to apply to messages
	 */
	private String delayExpression;

	/**
	 * A custom routing key when publishing messages; default is the destination name; suffixed by "-partition" when partitioned
	 */
	private String routingKeyExpression;

	public void setRequestHeaderPatterns(String[] requestHeaderPatterns) {
		this.requestHeaderPatterns = requestHeaderPatterns;
	}

	public String[] getRequestHeaderPatterns() {
		return requestHeaderPatterns;
	}

	public void setCompress(boolean compress) {
		this.compress = compress;
	}

	public boolean isCompress() {
		return compress;
	}

	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	public MessageDeliveryMode getDeliveryMode() {
		return deliveryMode;
	}

	public String[] getReplyHeaderPatterns() {
		return replyHeaderPatterns;
	}

	public void setReplyHeaderPatterns(String[] replyHeaderPatterns) {
		this.replyHeaderPatterns = replyHeaderPatterns;
	}

	public boolean isBatchingEnabled() {
		return batchingEnabled;
	}

	public void setBatchingEnabled(boolean batchingEnabled) {
		this.batchingEnabled = batchingEnabled;
	}

	@Min(value = 1, message = "Batch Size should be greater than zero.")
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	@Min(value = 1, message = "Batch Buffer Limit should be greater than zero.")
	public int getBatchBufferLimit() {
		return batchBufferLimit;
	}

	public void setBatchBufferLimit(int batchBufferLimit) {
		this.batchBufferLimit = batchBufferLimit;
	}

	@Min(value = 1, message = "Batch Timeout should be greater than zero.")
	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public boolean isTransacted() {
		return this.transacted;
	}

	public void setTransacted(boolean transacted) {
		this.transacted = transacted;
	}

	public String getDelayExpression() {
		return this.delayExpression;
	}

	public void setDelayExpression(String delayExpression) {
		this.delayExpression = delayExpression;
	}

	public String getRoutingKeyExpression() {
		return this.routingKeyExpression;
	}

	public void setRoutingKeyExpression(String routingKeyExpression) {
		this.routingKeyExpression = routingKeyExpression;
	}

}
