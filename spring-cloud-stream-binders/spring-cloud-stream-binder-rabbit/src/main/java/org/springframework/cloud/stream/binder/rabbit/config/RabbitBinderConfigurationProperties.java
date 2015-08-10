/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.config;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author David Turanski
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.binder.rabbit.default")
class RabbitBinderConfigurationProperties {

	private AcknowledgeMode acknowledgeMode;

	private int backOffInitialInterval;

	private int backOffMaxInterval;

	private double backOffMultiplier;

	private boolean transacted;

	private boolean concurrency;

	private MessageDeliveryMode deliveryMode;

	private boolean requeueRejected;

	private int maxAttempts;

	private int maxConcurrency;

	private int prefetchCount;

	private String prefix;

	private String[] replyHeaderPatterns;

	private String[] requestHeaderPatterns;

	private int txSize;

	private boolean autoBindDLQ;

	private boolean republishToDLQ;

	private boolean batchingEnabled;

	private int batchSize;

	private int batchBufferLimit;

	private int batchTimeout;

	private boolean compress;

	private int compressionLevel;

	private boolean durableSubscription;

	public AcknowledgeMode getAcknowledgeMode() {
		return acknowledgeMode;
	}

	public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
		this.acknowledgeMode = acknowledgeMode;
	}

	public int getBackOffInitialInterval() {
		return backOffInitialInterval;
	}

	public void setBackOffInitialInterval(int backOffInitialInterval) {
		this.backOffInitialInterval = backOffInitialInterval;
	}

	public int getBackOffMaxInterval() {
		return backOffMaxInterval;
	}

	public void setBackOffMaxInterval(int backOffMaxInterval) {
		this.backOffMaxInterval = backOffMaxInterval;
	}

	public double getBackOffMultiplier() {
		return backOffMultiplier;
	}

	public void setBackOffMultiplier(double backOffMultiplier) {
		this.backOffMultiplier = backOffMultiplier;
	}

	public boolean isTransacted() {
		return transacted;
	}

	public void setTransacted(boolean transacted) {
		this.transacted = transacted;
	}

	public boolean isConcurrency() {
		return concurrency;
	}

	public void setConcurrency(boolean concurrency) {
		this.concurrency = concurrency;
	}

	public MessageDeliveryMode getDeliveryMode() {
		return deliveryMode;
	}

	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	public boolean isRequeueRejected() {
		return requeueRejected;
	}

	public void setRequeueRejected(boolean requeueRejected) {
		this.requeueRejected = requeueRejected;
	}

	public int getMaxAttempts() {
		return maxAttempts;
	}

	public void setMaxAttempts(int maxAttempts) {
		this.maxAttempts = maxAttempts;
	}

	public int getMaxConcurrency() {
		return maxConcurrency;
	}

	public void setMaxConcurrency(int maxConcurrency) {
		this.maxConcurrency = maxConcurrency;
	}

	public int getPrefetchCount() {
		return prefetchCount;
	}

	public void setPrefetchCount(int prefetchCount) {
		this.prefetchCount = prefetchCount;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String[] getReplyHeaderPatterns() {
		return replyHeaderPatterns;
	}

	public void setReplyHeaderPatterns(String[] replyHeaderPatterns) {
		this.replyHeaderPatterns = replyHeaderPatterns;
	}

	public String[] getRequestHeaderPatterns() {
		return requestHeaderPatterns;
	}

	public void setRequestHeaderPatterns(String[] requestHeaderPatterns) {
		this.requestHeaderPatterns = requestHeaderPatterns;
	}

	public int getTxSize() {
		return txSize;
	}

	public void setTxSize(int txSize) {
		this.txSize = txSize;
	}

	public boolean isAutoBindDLQ() {
		return autoBindDLQ;
	}

	public void setAutoBindDLQ(boolean autoBindDLQ) {
		this.autoBindDLQ = autoBindDLQ;
	}

	public boolean isRepublishToDLQ() {
		return republishToDLQ;
	}

	public void setRepublishToDLQ(boolean republishToDLQ) {
		this.republishToDLQ = republishToDLQ;
	}

	public boolean isBatchingEnabled() {
		return batchingEnabled;
	}

	public void setBatchingEnabled(boolean batchingEnabled) {
		this.batchingEnabled = batchingEnabled;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getBatchBufferLimit() {
		return batchBufferLimit;
	}

	public void setBatchBufferLimit(int batchBufferLimit) {
		this.batchBufferLimit = batchBufferLimit;
	}

	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public boolean isCompress() {
		return compress;
	}

	public void setCompress(boolean compress) {
		this.compress = compress;
	}

	public int getCompressionLevel() {
		return compressionLevel;
	}

	public void setCompressionLevel(int compressionLevel) {
		this.compressionLevel = compressionLevel;
	}

	public boolean isDurableSubscription() {
		return durableSubscription;
	}

	public void setDurableSubscription(boolean durableSubscription) {
		this.durableSubscription = durableSubscription;
	}
}
