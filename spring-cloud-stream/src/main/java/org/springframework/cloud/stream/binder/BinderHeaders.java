/*
 * Copyright 2015-2017 the original author or authors.
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

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.messaging.MessageHeaders;

/**
 * Spring Integration message headers for Spring Cloud Stream.
 *
 * @author Gary Russell
 * @author David Turanski
 * @author Soby Chacko
 */
public final class BinderHeaders {

	/**
	 * Indicates the original content type of a message that has been transformed in a
	 * native transport format.
	 */
	public static final String BINDER_ORIGINAL_CONTENT_TYPE = "originalContentType";

	/**
	 * The headers that will be propagated, by default, by binder implementations that
	 * have no inherent header support (by embedding the headers in the payload).
	 */
	public static final String[] STANDARD_HEADERS = new String[] {
			IntegrationMessageHeaderAccessor.CORRELATION_ID,
			IntegrationMessageHeaderAccessor.SEQUENCE_SIZE,
			IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, MessageHeaders.CONTENT_TYPE,
			BINDER_ORIGINAL_CONTENT_TYPE };

	private static final String PREFIX = "scst_";

	/**
	 * Indicates the target partition of an outbound message. Binders must observe this
	 * value when sending data on the transport. This header is internally set by the
	 * framework when partitioning is configured. It may be overridden by
	 * {@link BinderHeaders#PARTITION_OVERRIDE} if set by the user.
	 */
	public static final String PARTITION_HEADER = PREFIX + "partition";

	/**
	 * Indicates the target partition of an outbound message. Overrides any partition
	 * selected by the binder. This header takes precedence over
	 * {@link BinderHeaders#PARTITION_HEADER}.
	 */
	public static final String PARTITION_OVERRIDE = PREFIX + "partitionOverride";

	/**
	 * Indicates that an incoming message has native headers.
	 * @since 2.0
	 */
	public static final String NATIVE_HEADERS_PRESENT = PREFIX + "nativeHeadersPresent";

	/**
	 * Indicates the Spring Cloud Stream version. Used for determining if legacy content
	 * type check supported or not.
	 * @since 2.0
	 */
	public static final String SCST_VERSION = PREFIX + "version";

	private BinderHeaders() {
		super();
	}

}
