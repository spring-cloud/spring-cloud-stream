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

import java.lang.reflect.Method;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * This class contains utility methods for handling {@link StreamListener} annotated bean
 * methods.
 *
 * @author Ilayaperumal Gopinathan
 */
public final class StreamListenerMethodUtils {

	private StreamListenerMethodUtils() {
		throw new IllegalStateException("Can't instantiate a utility class");
	}

	protected static int inputAnnotationCount(Method method) {
		int inputAnnotationCount = 0;
		for (int parameterIndex = 0; parameterIndex < method
				.getParameterTypes().length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method,
					parameterIndex);
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				inputAnnotationCount++;
			}
		}
		return inputAnnotationCount;
	}

	protected static int outputAnnotationCount(Method method) {
		int outputAnnotationCount = 0;
		for (int parameterIndex = 0; parameterIndex < method
				.getParameterTypes().length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method,
					parameterIndex);
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				outputAnnotationCount++;
			}
		}
		return outputAnnotationCount;
	}

	protected static void validateStreamListenerMethod(Method method,
			int inputAnnotationCount, int outputAnnotationCount,
			String methodAnnotatedInboundName, String methodAnnotatedOutboundName,
			boolean isDeclarative, String condition) {
		int methodArgumentsLength = method.getParameterTypes().length;
		if (!isDeclarative) {
			Assert.isTrue(inputAnnotationCount == 0 && outputAnnotationCount == 0,
					StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
		if (StringUtils.hasText(methodAnnotatedInboundName)
				&& StringUtils.hasText(methodAnnotatedOutboundName)) {
			Assert.isTrue(inputAnnotationCount == 0 && outputAnnotationCount == 0,
					StreamListenerErrorMessages.INVALID_INPUT_OUTPUT_METHOD_PARAMETERS);
		}
		if (StringUtils.hasText(methodAnnotatedInboundName)) {
			Assert.isTrue(inputAnnotationCount == 0,
					StreamListenerErrorMessages.INVALID_INPUT_VALUES);
			Assert.isTrue(outputAnnotationCount == 0,
					StreamListenerErrorMessages.INVALID_INPUT_VALUE_WITH_OUTPUT_METHOD_PARAM);
		}
		else {
			Assert.isTrue(inputAnnotationCount >= 1,
					StreamListenerErrorMessages.NO_INPUT_DESTINATION);
		}
		if (StringUtils.hasText(methodAnnotatedOutboundName)) {
			Assert.isTrue(outputAnnotationCount == 0,
					StreamListenerErrorMessages.INVALID_OUTPUT_VALUES);
		}
		if (!Void.TYPE.equals(method.getReturnType())) {
			Assert.isTrue(!StringUtils.hasText(condition),
					StreamListenerErrorMessages.CONDITION_ON_METHOD_RETURNING_VALUE);
		}
		if (isDeclarative) {
			Assert.isTrue(!StringUtils.hasText(condition),
					StreamListenerErrorMessages.CONDITION_ON_DECLARATIVE_METHOD);
			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forExecutable(method,
						parameterIndex);
				if (methodParameter.hasParameterAnnotation(Input.class)) {
					String inboundName = (String) AnnotationUtils.getValue(
							methodParameter.getParameterAnnotation(Input.class));
					Assert.isTrue(StringUtils.hasText(inboundName),
							StreamListenerErrorMessages.INVALID_INBOUND_NAME);
				}
				if (methodParameter.hasParameterAnnotation(Output.class)) {
					String outboundName = (String) AnnotationUtils.getValue(
							methodParameter.getParameterAnnotation(Output.class));
					Assert.isTrue(StringUtils.hasText(outboundName),
							StreamListenerErrorMessages.INVALID_OUTBOUND_NAME);
				}
			}
			if (methodArgumentsLength > 1) {
				Assert.isTrue(
						inputAnnotationCount
								+ outputAnnotationCount == methodArgumentsLength,
						StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
			}
		}

		if (!method.getReturnType().equals(Void.TYPE)) {
			if (!StringUtils.hasText(methodAnnotatedOutboundName)) {
				if (outputAnnotationCount == 0) {
					throw new IllegalArgumentException(
							StreamListenerErrorMessages.RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
				}
				Assert.isTrue((outputAnnotationCount == 1),
						StreamListenerErrorMessages.RETURN_TYPE_MULTIPLE_OUTBOUND_SPECIFIED);
			}
		}
	}

	protected static void validateStreamListenerMessageHandler(Method method) {
		int methodArgumentsLength = method.getParameterTypes().length;
		if (methodArgumentsLength > 1) {
			int numAnnotatedMethodParameters = 0;
			int numPayloadAnnotations = 0;
			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forExecutable(method,
						parameterIndex);
				if (methodParameter.hasParameterAnnotations()) {
					numAnnotatedMethodParameters++;
				}
				if (methodParameter.hasParameterAnnotation(Payload.class)) {
					numPayloadAnnotations++;
				}
			}
			if (numPayloadAnnotations > 0) {
				Assert.isTrue(
						methodArgumentsLength == numAnnotatedMethodParameters
								&& numPayloadAnnotations <= 1,
						StreamListenerErrorMessages.AMBIGUOUS_MESSAGE_HANDLER_METHOD_ARGUMENTS);
			}
		}
	}

	protected static String getOutboundBindingTargetName(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()),
					StreamListenerErrorMessages.ATLEAST_ONE_OUTPUT);
			Assert.isTrue(sendTo.value().length == 1,
					StreamListenerErrorMessages.SEND_TO_MULTIPLE_DESTINATIONS);
			Assert.hasText(sendTo.value()[0],
					StreamListenerErrorMessages.SEND_TO_EMPTY_DESTINATION);
			return sendTo.value()[0];
		}
		Output output = AnnotationUtils.findAnnotation(method, Output.class);
		if (output != null) {
			Assert.isTrue(StringUtils.hasText(output.value()),
					StreamListenerErrorMessages.ATLEAST_ONE_OUTPUT);
			return output.value();
		}
		return null;
	}

}
