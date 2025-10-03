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

package org.springframework.cloud.stream.config;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
class ArgumentResolversTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void smartPayloadArgumentResolver() throws Exception {

		SmartPayloadArgumentResolver resolver = new SmartPayloadArgumentResolver(new TestMessageConverter());

		Object payload = "hello".getBytes();
		GenericMessage message = new GenericMessage(payload);
		MethodParameter parameter = new MethodParameter(getMethod("byteArray", byte[].class), 0);
		Object resolvedArgument = resolver.resolveArgument(parameter, message);
		assertThat(resolvedArgument).isSameAs(payload);

		parameter = new MethodParameter(getMethod("object", Object.class), 0);
		resolvedArgument = resolver.resolveArgument(parameter, message);
		assertThat(resolvedArgument).isInstanceOf(Message.class);

		payload = new LinkedHashMap<>();
		message = new GenericMessage(payload);
		parameter = new MethodParameter(getMethod("map", Map.class), 0);
		resolvedArgument = resolver.resolveArgument(parameter, message);
		assertThat(resolvedArgument).isSameAs(payload);

		parameter = new MethodParameter(getMethod("object", Object.class), 0);
		resolvedArgument = resolver.resolveArgument(parameter, message);
		assertThat(resolvedArgument).isInstanceOf(Message.class);
	}

	private Method getMethod(String name, Class<?> parameter) {
		return ReflectionUtils.findMethod(this.getClass(), name, parameter);
	}

	public void byteArray(byte[] p) {

	}

	public void byteArrayMessage(Message<byte[]> p) {

	}

	public void object(Object p) {

	}
	@SuppressWarnings("rawtypes")
	public void map(Map p) {

	}

	/*
	 * The whole point of this converter is to return something other
	 * then what is being resolved to simply validate when it is invoked
	 * vs. when it is not.
	 */
	private static class TestMessageConverter implements MessageConverter {

		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass) {
			return message;
		}

		@Override
		public Message<?> toMessage(Object payload, MessageHeaders headers) {
			return new GenericMessage<>(payload);
		}

	}
}
