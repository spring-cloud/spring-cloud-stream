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

package org.springframework.cloud.stream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.messaging.MessageHeaders;

/**
 * Contains properties for Spring Integration settings.
 *
 * @author Marius Bogoevici
 * @since 1.2.3
 */
@ConfigurationProperties("spring.cloud.stream.integration")
public class SpringIntegrationProperties {

	/**
	 * Message header names that will NOT be copied from the inbound message.
	 */
	private String[] messageHandlerNotPropagatedHeaders = new String[] {
			MessageHeaders.CONTENT_TYPE };

	public String[] getMessageHandlerNotPropagatedHeaders() {
		return this.messageHandlerNotPropagatedHeaders;
	}

	public void setMessageHandlerNotPropagatedHeaders(
			String[] messageHandlerNotPropagatedHeaders) {
		this.messageHandlerNotPropagatedHeaders = messageHandlerNotPropagatedHeaders;
	}

}
