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

package org.springframework.cloud.stream.reactive;

import org.springframework.cloud.stream.binding.StreamAnnotationErrorMessages;

/**
 * @author Soby Chacko
 * @since 1.3.0
 */
abstract class StreamEmitterErrorMessages extends StreamAnnotationErrorMessages {

	static final String INVALID_OUTPUT_METHOD_PARAMETERS = "@Output annotations are not permitted on "
			+ "method parameters while using the @StreamEmitter and a method-level output specification";
	static final String NO_OUTPUT_SPECIFIED = "No method level or parameter level @Output annotations are detected. "
			+ "@StreamEmitter requires a method or parameter level @Output annotation.";

	// @checkstyle:off
	static final String CANNOT_CONVERT_RETURN_TYPE_TO_ANY_AVAILABLE_RESULT_ADAPTERS = "No suitable adapters are found that can convert the return type";

	// @checkstyle:on

	private static final String PREFIX = "A method annotated with @StreamEmitter ";
	static final String RETURN_TYPE_NO_OUTBOUND_SPECIFIED = PREFIX
			+ "having a return type should also have an outbound target specified at the method level.";
	static final String RETURN_TYPE_METHOD_ARGUMENTS = PREFIX
			+ "having a return type should not have any method arguments";
	static final String OUTPUT_ANNOTATION_MISSING_ON_METHOD_PARAMETERS_VOID_RETURN_TYPE = PREFIX
			+ "and void return type without method level @Output annotation requires @Output on each of the method parameter";
	static final String INPUT_ANNOTATIONS_ARE_NOT_ALLOWED = PREFIX
			+ "cannot contain @Input annotations";

}
