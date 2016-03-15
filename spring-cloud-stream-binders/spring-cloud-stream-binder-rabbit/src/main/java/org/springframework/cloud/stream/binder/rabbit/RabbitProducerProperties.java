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

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.cloud.stream.binder.ProducerProperties;

/**
 * @author Marius Bogoevici
 */
public class RabbitProducerProperties extends ProducerProperties {

	private String prefix = "";

	private String[] requestHeaderPatterns = new String[] {"STANDARD_REQUEST_HEADERS", "*"};

	private boolean autoBindDlq;

	private boolean compress;

	private MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;

	private String[] replyHeaderPatterns = new String[] {"STANDARD_REQUEST_HEADERS", "*"};

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public void setRequestHeaderPatterns(String[] requestHeaderPatterns) {
		this.requestHeaderPatterns = requestHeaderPatterns;
	}

	public String[] getRequestHeaderPatterns() {
		return requestHeaderPatterns;
	}

	public void setAutoBindDlq(boolean autoBindDlq) {
		this.autoBindDlq = autoBindDlq;
	}

	public boolean isAutoBindDlq() {
		return autoBindDlq;
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
}
