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
public interface StreamListenerErrorMessages {


	public static final String INPUT_AT_STREAM_LISTENER = "A @StreamListener may never be annotated with @Input. If it should listen to a specific input, " +
			"use the value of @StreamListener instead.";

	public static final String RETURN_TYPE_NO_OUTBOUND_SPECIFIED = "StreamListener method with return type should have outbound target specified";

	public static final String RETURN_TYPE_MULTIPLE_OUTBOUND_SPECIFIED = "StreamListener method with return type should have only one outbound target specified";

	public static final String INVALID_INBOUND_NAME = "@Input annotation should always be associated with a valid inbound name";

	public static final String INVALID_OUTBOUND_NAME = "@Output annotation should always be associated with a valid outbound name";

	public static final String ATLEAST_ONE_OUTPUT = "At least one output must be specified";

	public static final String SEND_TO_MULTIPLE_DESTINATIONS = "Multiple destinations cannot be specified";

	public static final String SEND_TO_EMPTY_DESTINATION = "An empty destination cannot be specified";

	public static final String INVALID_MESSAGE_HANDLER_METHOD_PARAMS = "@Input or @Output annotation is not supported as method parameter in StreamListener method with " +
			"message handler mapping";

	public static final String INVALID_INPUT_OUTPUT_METHOD_PARAMETERS = "@Input or @Output annotations are not permitted as " +
			"method parameters when both inbound and outbound values are set as method annotated values";

	public static final String NO_INPUT_DESTINATION = "No input destination is configured. Use either a @StreamListener attribute or @Input";

	public static final String INVALID_DECLARATIVE_METHOD_PARAMETERS = "Declarative StreamListener method should only have inbound or outbound targets as method parameters";

	public static final String AMBIGUOUS_MESSAGE_HANDLER_METHOD_ARGUMENTS = "Ambiguous method arguments for the StreamListener method";

	public static final String INVALID_INPUT_VALUES = "Cannot set both StreamListener attribute and @Input annotation as method parameter";

	public static final String INVALID_INPUT_VALUE_WITH_OUTPUT_METHOD_PARAM = "Cannot set StreamListener attribute when using" +
			" @Output annotation as method parameter. Use @Input method parameter annotation to specify inbound value instead";

	public static final String INVALID_OUTPUT_VALUES = "Cannot set both Output (@Output/@SendTo) method annotation value" +
			" and @Output annotation as a method parameter";

	public static final String INVALID_DECLARATIVE_METHOD_PARAMS = "Declarative StreamListener method should only have inbound or outbound targets as method parameters";

	public static final String TARGET_BEAN_NOT_EXISTS = "Target bean doesn't exist for the bound element name";
}
