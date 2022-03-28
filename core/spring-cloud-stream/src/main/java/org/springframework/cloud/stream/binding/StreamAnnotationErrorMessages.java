/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.binding;

/**
 * @author Soby Chacko
 */
public abstract class StreamAnnotationErrorMessages {

	/**
	 * Annotation error message when no output was passed.
	 */
	public static final String ATLEAST_ONE_OUTPUT = "At least one output must be specified";

	/**
	 * Annotation error message when multiple destinations were set.
	 */
	public static final String SEND_TO_MULTIPLE_DESTINATIONS = "Multiple destinations cannot be specified";

	/**
	 * Annotation error message when an empty destination was specified.
	 */
	public static final String SEND_TO_EMPTY_DESTINATION = "An empty destination cannot be specified";

	/**
	 * Annotation error message when an invalid outbound name was set.
	 */
	public static final String INVALID_OUTBOUND_NAME = "The @Output annotation must have the name of an input as value";

}
