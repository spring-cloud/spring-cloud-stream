/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.bus;

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.messaging.MessageHeaders;


/**
 * Spring Integration message headers for XD.
 *
 * @author Gary Russell
 */
public final class XdHeaders {

	public static final String XD_REPLY_CHANNEL = "xdReplyChannel";

	public static final String XD_HISTORY = "xdHistory";

	/*
	 * no xd prefix for backwards compatibility
	 */
	public static final String XD_ORIGINAL_CONTENT_TYPE = "originalContentType";

	/*
	 * no xd prefix for backwards compatibility
	 */
	public static final String REPLY_TO = "replyTo";

	/**
	 * The headers that will be propagated, by default, by message bus implementations
	 * that have no inherent header support (by embedding the headers in the payload).
	 */
	public static final String[] STANDARD_HEADERS = new String[] {
		IntegrationMessageHeaderAccessor.CORRELATION_ID,
		IntegrationMessageHeaderAccessor.SEQUENCE_SIZE,
		IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER,
		XD_REPLY_CHANNEL,
		MessageHeaders.CONTENT_TYPE,
		XD_ORIGINAL_CONTENT_TYPE,
		REPLY_TO,
		XD_HISTORY
	};

	private XdHeaders() {
	}

}
