/*
 * Copyright 2018-2019 the original author or authors.
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

/**
 * When using a {@code PollableMessageSource} throw this exception to cause the current
 * message to be requeued in the broker so that it will be redelivered on the next poll.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
public class RequeueCurrentMessageException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RequeueCurrentMessageException() {
		super();
	}

	public RequeueCurrentMessageException(String message, Throwable cause) {
		super(message, cause);
	}

	public RequeueCurrentMessageException(String message) {
		super(message);
	}

	public RequeueCurrentMessageException(Throwable cause) {
		super(cause);
	}

}
