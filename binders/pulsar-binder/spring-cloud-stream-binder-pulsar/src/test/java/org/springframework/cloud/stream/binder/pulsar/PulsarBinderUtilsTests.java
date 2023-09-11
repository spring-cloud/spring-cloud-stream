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
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.ProducerAccessMode;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import org.springframework.cloud.stream.binder.pulsar.properties.ConsumerConfigProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.ProducerConfigProperties;
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
			var pulsarConsumerProperties = mock(PulsarConsumerProperties.class, Mockito.RETURNS_DEEP_STUBS);
			when(pulsarConsumerProperties.getSubscription().getName()).thenReturn("my-sub");
			assertThat(PulsarBinderUtils.subscriptionName(pulsarConsumerProperties, consumerDestination))
					.isEqualTo("my-sub");
		}

		@Test
		void generatesValueWhenNotSetAsProperty() {
			var consumerDestination = mock(ConsumerDestination.class);
			var pulsarConsumerProperties = mock(PulsarConsumerProperties.class, Mockito.RETURNS_DEEP_STUBS);
			when(pulsarConsumerProperties.getSubscription().getName()).thenReturn(null);
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

	@Nested
	class MergeProducerPropertiesTests {

		private static final Consumer<ProducerConfigProperties> SET_NO_PROPS = (__) -> {
		};

		@Test
		void noPropsSpecified() {
			doMergeProducerPropertiesTest(SET_NO_PROPS, SET_NO_PROPS, Collections.emptyMap());
		}

		@Test
		void basePropSpecifiedAtBinderLevelOnly() {
			doMergeProducerPropertiesTest((binderProps) -> binderProps.setAccessMode(ProducerAccessMode.Exclusive),
					SET_NO_PROPS, Map.of("accessMode", ProducerAccessMode.Exclusive));
		}

		@Test
		void basePropSpecifiedAtBindingLevelOnly() {
			doMergeProducerPropertiesTest(SET_NO_PROPS,
					(bindingProps) -> bindingProps.setAccessMode(ProducerAccessMode.Exclusive),
					Map.of("accessMode", ProducerAccessMode.Exclusive));
		}

		@Test
		void basePropSpecifiedAtBinderAndBindingLevel() {
			doMergeProducerPropertiesTest(
					(binderProps) -> binderProps.setAccessMode(ProducerAccessMode.ExclusiveWithFencing),
					(bindingProps) -> bindingProps.setAccessMode(ProducerAccessMode.Exclusive),
					Map.of("accessMode", ProducerAccessMode.Exclusive));
		}

		@Test
		void basePropSpecifiedWithSameValueAsDefault() {
			doMergeProducerPropertiesTest((binderProps) -> binderProps.setAccessMode(ProducerAccessMode.Shared),
					(bindingProps) -> bindingProps.setAccessMode(ProducerAccessMode.Shared), Collections.emptyMap());
		}

		@Test
		void extPropSpecifiedAtBinderLevelOnly() {
			doMergeProducerPropertiesTest((binderProps) -> binderProps.setMaxPendingMessages(1200), SET_NO_PROPS,
					Map.of("maxPendingMessages", 1200));
		}

		@Test
		void extPropSpecifiedAtBindingLevelOnly() {
			doMergeProducerPropertiesTest(SET_NO_PROPS, (bindingProps) -> bindingProps.setMaxPendingMessages(1200),
					Map.of("maxPendingMessages", 1200));
		}

		@Test
		void extPropSpecifiedAtBinderAndBindingLevel() {
			doMergeProducerPropertiesTest((binderProps) -> binderProps.setMaxPendingMessages(1100),
					(bindingProps) -> bindingProps.setMaxPendingMessages(1200), Map.of("maxPendingMessages", 1200));
		}

		@Test
		void extPropSpecifiedWithSameValueAsDefault() {
			doMergeProducerPropertiesTest((binderProps) -> binderProps.setMaxPendingMessages(1000),
					(bindingProps) -> bindingProps.setMaxPendingMessages(1000), Collections.emptyMap());
		}

		@Test
		void baseAndExtPropsAreCombined() {
			doMergeProducerPropertiesTest((binderProps) -> binderProps.setAccessMode(ProducerAccessMode.Exclusive),
					(bindingProps) -> bindingProps.setMaxPendingMessages(1200),
					Map.of("accessMode", ProducerAccessMode.Exclusive, "maxPendingMessages", 1200));
		}

		void doMergeProducerPropertiesTest(Consumer<ProducerConfigProperties> binderPropsCustomizer,
				Consumer<ProducerConfigProperties> bindingPropsCustomizer, Map<String, Object> expectedProps) {
			var binderProducerProps = new ProducerConfigProperties();
			binderPropsCustomizer.accept(binderProducerProps);
			var bindingProducerProps = new ProducerConfigProperties();
			bindingPropsCustomizer.accept(bindingProducerProps);
			var mergedProps = PulsarBinderUtils.mergeModifiedProducerProperties(binderProducerProps,
					bindingProducerProps);
			assertThat(mergedProps).containsExactlyInAnyOrderEntriesOf(expectedProps);
		}

	}

	@Nested
	class MergeConsumerPropertiesTests {

		private static final Consumer<ConsumerConfigProperties> SET_NO_PROPS = (__) -> {
		};

		@Test
		void noPropsSpecified() {
			doMergeConsumerPropertiesTest(SET_NO_PROPS, SET_NO_PROPS, Collections.emptyMap());
		}

		@Test
		void basePropSpecifiedAtBinderLevelOnly() {
			doMergeConsumerPropertiesTest((binderProps) -> binderProps.setPriorityLevel(1000), SET_NO_PROPS,
					Map.of("priorityLevel", 1000));
		}

		@Test
		void basePropSpecifiedAtBindingLevelOnly() {
			doMergeConsumerPropertiesTest(SET_NO_PROPS, (bindingProps) -> bindingProps.setPriorityLevel(1000),
					Map.of("priorityLevel", 1000));
		}

		@Test
		void basePropSpecifiedAtBinderAndBindingLevel() {
			doMergeConsumerPropertiesTest((binderProps) -> binderProps.setPriorityLevel(2000),
					(bindingProps) -> bindingProps.setPriorityLevel(1000), Map.of("priorityLevel", 1000));
		}

		@Test
		void basePropSpecifiedWithSameValueAsDefault() {
			doMergeConsumerPropertiesTest((binderProps) -> binderProps.setPriorityLevel(0),
					(bindingProps) -> bindingProps.setPriorityLevel(0), Collections.emptyMap());
		}

		@Test
		void extPropSpecifiedAtBinderLevelOnly() {
			doMergeConsumerPropertiesTest((binderProps) -> binderProps.setReceiverQueueSize(1200), SET_NO_PROPS,
					Map.of("receiverQueueSize", 1200));
		}

		@Test
		void extPropSpecifiedAtBindingLevelOnly() {
			doMergeConsumerPropertiesTest(SET_NO_PROPS, (bindingProps) -> bindingProps.setReceiverQueueSize(1200),
					Map.of("receiverQueueSize", 1200));
		}

		@Test
		void extPropSpecifiedAtBinderAndBindingLevel() {
			doMergeConsumerPropertiesTest((binderProps) -> binderProps.setReceiverQueueSize(1100),
					(bindingProps) -> bindingProps.setReceiverQueueSize(1200), Map.of("receiverQueueSize", 1200));
		}

		@Test
		void extPropSpecifiedWithSameValueAsDefault() {
			doMergeConsumerPropertiesTest((binderProps) -> binderProps.setReceiverQueueSize(1000),
					(bindingProps) -> bindingProps.setReceiverQueueSize(1000), Collections.emptyMap());
		}

		@Test
		void baseAndExtPropsAreCombined() {
			doMergeConsumerPropertiesTest((binderProps) -> binderProps.setPriorityLevel(1000),
					(bindingProps) -> bindingProps.setReceiverQueueSize(1200),
					Map.of("priorityLevel", 1000, "receiverQueueSize", 1200));
		}

		void doMergeConsumerPropertiesTest(Consumer<ConsumerConfigProperties> binderPropsCustomizer,
				Consumer<ConsumerConfigProperties> bindingPropsCustomizer, Map<String, Object> expectedProps) {
			var binderConsumerProps = new ConsumerConfigProperties();
			binderPropsCustomizer.accept(binderConsumerProps);
			var bindingConsumerProps = new ConsumerConfigProperties();
			bindingPropsCustomizer.accept(bindingConsumerProps);
			var mergedProps = PulsarBinderUtils.mergeModifiedConsumerProperties(binderConsumerProps,
					bindingConsumerProps);
			assertThat(mergedProps).containsExactlyInAnyOrderEntriesOf(expectedProps);
		}

	}

}
