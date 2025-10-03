/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.cloud.stream.function;

/**
 * Interfaces which defines constants used for naming conventions used in bindings
 * of multiple functions or functions with multiple inputs and outputs.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 * @since 3.0
 *
 */
public final class FunctionConstants {

	private FunctionConstants() {

	}

	/**
	 * Delimiter used for functional binding naming convention.
	 */
	public static final String DELIMITER = "-";

	/**
	 * Suffix used for functional binding naming convention.
	 */
	public static final String DEFAULT_OUTPUT_SUFFIX = "out";

	/**
	 * Prefix used for functional binding naming convention.
	 */
	public static final String DEFAULT_INPUT_SUFFIX = "in";
}
