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

import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Common methods that can be used across various Stream annotations.
 *
 * @author Soby Chacko
 * @since 1.3.0
 */
public abstract class StreamAnnotationCommonMethodUtils {

	public static String getOutboundBindingTargetName(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()),
					StreamAnnotationErrorMessages.ATLEAST_ONE_OUTPUT);
			Assert.isTrue(sendTo.value().length == 1,
					StreamAnnotationErrorMessages.SEND_TO_MULTIPLE_DESTINATIONS);
			Assert.hasText(sendTo.value()[0],
					StreamAnnotationErrorMessages.SEND_TO_EMPTY_DESTINATION);
			return sendTo.value()[0];
		}
//		Output output = AnnotationUtils.findAnnotation(method, Output.class);
//		if (output != null) {
//			Assert.isTrue(StringUtils.hasText(output.value()),
//					StreamAnnotationErrorMessages.ATLEAST_ONE_OUTPUT);
//			return output.value();
//		}
		return null;
	}

	public static int outputAnnotationCount(Method method) {
		int outputAnnotationCount = 0;
		for (int parameterIndex = 0; parameterIndex < method
				.getParameterTypes().length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method,
					parameterIndex);
//			if (methodParameter.hasParameterAnnotation(Output.class)) {
//				outputAnnotationCount++;
//			}
		}
		return outputAnnotationCount;
	}

}
