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

package org.springframework.cloud.stream.binder.rabbit;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class RabbitConsumerProperties extends ConsumerProperties {

	private String prefix = "";

	private boolean transacted = false;

	private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private int maxConcurrency = 1;

	private int prefetch = 1;

	private String[] requestHeaderPatterns = new String[] {"STANDARD_REQUEST_HEADERS", "*"};

	private int txSize = 1;

	private boolean autoBindDlq = false;

	private boolean durableSubscription = true;

	private boolean republishToDlq = false;

	private boolean requeueRejected = true;

	private String replyHeaderPatterns;

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public boolean isTransacted() {
		return transacted;
	}

	public void setTransacted(boolean transacted) {
		this.transacted = transacted;
	}

	public AcknowledgeMode getAcknowledgeMode() {
		return acknowledgeMode;
	}

	public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
		Assert.notNull("Acknowledge mode cannot be null");
		this.acknowledgeMode = acknowledgeMode;
	}

	public int getMaxConcurrency() {
		return maxConcurrency;
	}

	public void setMaxConcurrency(int maxConcurrency) {
		this.maxConcurrency = maxConcurrency;
	}

	public int getPrefetch() {
		return prefetch;
	}

	public void setPrefetch(int prefetch) {
		this.prefetch = prefetch;
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

	public boolean isAutoBindDlq() {
		return autoBindDlq;
	}

	public void setAutoBindDlq(boolean autoBindDlq) {
		this.autoBindDlq = autoBindDlq;
	}

	public boolean isDurableSubscription() {
		return durableSubscription;
	}

	public void setDurableSubscription(boolean durableSubscription) {
		this.durableSubscription = durableSubscription;
	}

	public boolean isRepublishToDlq() {
		return republishToDlq;
	}

	public void setRepublishToDlq(boolean republishToDlq) {
		this.republishToDlq = republishToDlq;
	}

	public boolean isRequeueRejected() {
		return requeueRejected;
	}

	public void setRequeueRejected(boolean requeueRejected) {
		this.requeueRejected = requeueRejected;
	}

	public String getReplyHeaderPatterns() {
		return replyHeaderPatterns;
	}

	public void setReplyHeaderPatterns(String replyHeaderPatterns) {
		this.replyHeaderPatterns = replyHeaderPatterns;
	}
}
