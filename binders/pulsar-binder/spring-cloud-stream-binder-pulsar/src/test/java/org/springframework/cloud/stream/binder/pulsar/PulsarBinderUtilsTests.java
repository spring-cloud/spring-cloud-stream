/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.cloud.stream.binder.pulsar.properties.PulsarConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PulsarBinderUtils}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarBinderUtilsTests {

	@Nested
	class SubscriptionNameTests {

		@Test
		void respectsValueWhenSetAsProperty() {
			var consumerDestination = mock(ConsumerDestination.class);
			var pulsarConsumerProperties = mock(PulsarConsumerProperties.class);
			when(pulsarConsumerProperties.getSubscriptionName()).thenReturn("my-sub");
			assertThat(PulsarBinderUtils.subscriptionName(pulsarConsumerProperties, consumerDestination))
					.isEqualTo("my-sub");
		}

		@Test
		void generatesValueWhenNotSetAsProperty() {
			var consumerDestination = mock(ConsumerDestination.class);
			var pulsarConsumerProperties = mock(PulsarConsumerProperties.class);
			when(pulsarConsumerProperties.getSubscriptionName()).thenReturn(null);
			when(consumerDestination.getName()).thenReturn("my-topic");
			assertThat(PulsarBinderUtils.subscriptionName(pulsarConsumerProperties, consumerDestination))
					.startsWith("my-topic-anon-subscription-");
		}

	}

	@Nested
	class MergedPropertiesTests {

		@ParameterizedTest(name = "{0}")
		@MethodSource("mergePropertiesTestProvider")
		void mergePropertiesTest(String testName, Map<String, Object> baseProps, Map<String, Object> binderProps,
				Map<String, Object> bindingProps, Map<String, Object> expectedMergedProps) {
			assertThat(PulsarBinderUtils.mergePropertiesWithPrecedence(baseProps, binderProps, bindingProps))
					.containsExactlyInAnyOrderEntriesOf(expectedMergedProps);
		}

		// @formatter:off
		static Stream<Arguments> mergePropertiesTestProvider() {
			return Stream.of(
					arguments("binderLevelContainsSamePropAsBaseWithDiffValue",
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-binder"),
							Collections.emptyMap(),
							Map.of("foo", "foo-binder")),
					arguments("binderLevelContainsNewPropNotInBase",
							Collections.emptyMap(),
							Map.of("foo", "foo-binder"),
							Collections.emptyMap(),
							Map.of("foo", "foo-binder")),
					arguments("binderLevelContainsSamePropAsBaseWithSameValue",
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Collections.emptyMap()),
					arguments("bindingLevelContainsSamePropAsBaseWithDiffValue",
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Map.of("foo", "foo-binding"),
							Map.of("foo", "foo-binding")),
					arguments("bindingLevelContainsNewPropNotInBase",
							Collections.emptyMap(),
							Map.of("foo", "foo-binding"),
							Collections.emptyMap(),
							Map.of("foo", "foo-binding")),
					arguments("bindingLevelContainsSamePropAsBaseWithSameValue",
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Map.of("foo", "foo-base"),
							Collections.emptyMap()),
					arguments("bindingOverridesBinder",
							Map.of("bar", "bar-base"),
							Map.of("foo", "foo-binder"),
							Map.of("foo", "foo-binding"),
							Map.of("foo", "foo-binding")),
					arguments("binderOverridesBaseAndBindingOverridesBinder",
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-binder"),
							Map.of("foo", "foo-binding"),
							Map.of("foo", "foo-binding")),
					arguments("onlyBaseProps",
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Collections.emptyMap(),
							Collections.emptyMap()));
		}
		// @formatter:on

	}

}
