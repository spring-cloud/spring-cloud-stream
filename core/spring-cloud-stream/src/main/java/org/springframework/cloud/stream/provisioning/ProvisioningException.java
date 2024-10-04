/*
 * Copyright 2015-2024 the original author or authors.
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

package org.springframework.cloud.stream.provisioning;

import org.springframework.core.NestedRuntimeException;

/**
 * Generic unchecked exception to wrap middleware or technology specific exceptions.
 * Wrapped exceptions could be either checked or unchecked.
 *
 * See {@link NestedRuntimeException} for more usage details.
 *
 * @author Soby Chacko
 * @author Omer Celik
 */
public class ProvisioningException extends NestedRuntimeException {

	/**
	 * Constructor that takes a message.
	 * @param msg the detail message
	 */
	public ProvisioningException(String msg) {
		super(msg);
	}

	/**
	 * Constructor that takes a message and a root cause.
	 * @param msg the detail message
	 * @param cause the cause of the exception. This argument is generally expected to be
	 * middleware specific.
	 */
	public ProvisioningException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
