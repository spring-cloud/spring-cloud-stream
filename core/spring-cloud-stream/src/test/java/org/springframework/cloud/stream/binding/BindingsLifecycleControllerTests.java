/*
 * Copyright 2026-present the original author or authors.
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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.cloud.stream.binder.Binding;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link BindingsLifecycleController}.
 *
 * @author Oleg Zhurakousky
 */
class BindingsLifecycleControllerTests {

	@Test
	@SuppressWarnings("unchecked")
	void queryStatesSerializesSpelExpressionsAsStrings() {
		InputBindingLifecycle input = mock(InputBindingLifecycle.class);
		OutputBindingLifecycle output = mock(OutputBindingLifecycle.class);
		Binding<?> binding = new Binding<>() {
			@Override
			public void unbind() {
			}

			@Override
			public boolean isInput() {
				return false;
			}

			@Override
			public String getBindingName() {
				return "foo-out-0";
			}

			@Override
			public <P> P getExtension() {
				return (P) new Props();
			}
		};
		ReflectionTestUtils.setField(input, "inputBindings", List.of());
		ReflectionTestUtils.setField(output, "outputBindings", List.of(binding));

		BindingsLifecycleController controller = new BindingsLifecycleController(List.of(input), List.of(output));
		List<Map<String, Object>> states = controller.queryStates();

		Map<String, Object> extension = (Map<String, Object>) states.get(0).get("extension");
		assertThat(extension.get("delayExpression")).isEqualTo("30000");
		assertThat(extension.get("routingKeyExpression")).isEqualTo("headers['rk']");
	}

	public static class Props {

		private final org.springframework.expression.Expression delayExpression =
				new SpelExpressionParser().parseExpression("30000");

		private final org.springframework.expression.Expression routingKeyExpression =
				new SpelExpressionParser().parseExpression("headers['rk']");

		public org.springframework.expression.Expression getDelayExpression() {
			return this.delayExpression;
		}

		public org.springframework.expression.Expression getRoutingKeyExpression() {
			return this.routingKeyExpression;
		}

	}

}
