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

package org.springframework.cloud.stream.binding;

/**
 * @author Ilayaperumal Gopinathan
 */
public abstract class StreamListenerErrorMessages {

	public static final String INVALID_INBOUND_NAME = "The @Input annotation must have the name of an input as value";

	public static final String INVALID_OUTBOUND_NAME = "The @Output annotation must have the name of an input as value";

	public static final String ATLEAST_ONE_OUTPUT = "At least one output must be specified";

	public static final String SEND_TO_MULTIPLE_DESTINATIONS = "Multiple destinations cannot be specified";

	public static final String SEND_TO_EMPTY_DESTINATION = "An empty destination cannot be specified";

	public static final String INVALID_INPUT_OUTPUT_METHOD_PARAMETERS = "@Input or @Output annotations are not permitted on "
			+ "method parameters while using the @StreamListener value and a method-level output specification";

	public static final String NO_INPUT_DESTINATION = "No input destination is configured. Use either the @StreamListener value or @Input";

	public static final String AMBIGUOUS_MESSAGE_HANDLER_METHOD_ARGUMENTS = "Ambiguous method arguments for the StreamListener method";

	public static final String INVALID_INPUT_VALUES = "Cannot set both @StreamListener value and @Input annotation as method parameter";

	public static final String INVALID_INPUT_VALUE_WITH_OUTPUT_METHOD_PARAM = "Setting the @StreamListener value when using"
			+ " @Output annotation as method parameter is not permitted. Use @Input method parameter annotation to specify inbound value instead";

	public static final String INVALID_OUTPUT_VALUES = "Cannot set both output (@Output/@SendTo) method annotation value"
			+ " and @Output annotation as a method parameter";

	public static final String CONDITION_ON_DECLARATIVE_METHOD = "Cannot set a condition when using @StreamListener in declarative mode";

	public static final String CONDITION_ON_METHOD_RETURNING_VALUE = "Cannot set a condition for methods that return a value";

	public static final String MULTIPLE_VALUE_RETURNING_METHODS = "If multiple @StreamListener methods are listening to the same binding target, none of them may return a value";

	private static final String PREFIX = "A method annotated with @StreamListener ";

	public static final String INPUT_AT_STREAM_LISTENER = PREFIX
			+ "may never be annotated with @Input. "
			+ "If it should listen to a specific input, use the value of @StreamListener instead";

	public static final String RETURN_TYPE_NO_OUTBOUND_SPECIFIED = PREFIX
			+ "having a return type should also have an outbound target specified";

	public static final String RETURN_TYPE_MULTIPLE_OUTBOUND_SPECIFIED = PREFIX
			+ "having a return type should have only one outbound target specified";

	public static final String INVALID_DECLARATIVE_METHOD_PARAMETERS = PREFIX
			+ "may use @Input or @Output annotations only in declarative mode and for parameters that are binding targets or convertible from binding targets.";
}
