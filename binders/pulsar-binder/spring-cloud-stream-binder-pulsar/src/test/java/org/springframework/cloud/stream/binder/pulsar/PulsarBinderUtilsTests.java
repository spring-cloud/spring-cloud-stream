/*
 * Copyright 2023-2024 the original author or authors.
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
class PulsarBinderUtilsTests {

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
		void mergePropertiesTest(String testName, boolean includeDefaults, Map<String, Object> globalProps, Map<String, Object> binderProps,
				Map<String, Object> bindingProps, Map<String, Object> expectedMergedProps) {
			assertThat(PulsarBinderUtils.mergePropertiesWithPrecedence(globalProps, binderProps, bindingProps, includeDefaults))
					.containsExactlyInAnyOrderEntriesOf(expectedMergedProps);
		}

		// @formatter:off
		static Stream<Arguments> mergePropertiesTestProvider() {
			return Stream.of(
					arguments("noProps",
							true,
							Collections.emptyMap(),
							Collections.emptyMap(),
							Collections.emptyMap(),
							Collections.emptyMap()),
					arguments("allSamePropsWithoutDefaults",
							false,
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base"),
							Collections.emptyMap()),
					arguments("allSamePropsWithDefaults",
							true,
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base")),
					arguments("onlyBasePropsWithoutDefaults",
							false,
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Collections.emptyMap(),
							Collections.emptyMap()),
					arguments("onlyBasePropsWithDefaults",
							true,
							Map.of("foo", "foo-base"),
							Collections.emptyMap(),
							Collections.emptyMap(),
							Map.of("foo", "foo-base")),
					arguments("onlyBinderProps",
							true,
							Collections.emptyMap(),
							Map.of("foo", "foo-binder"),
							Collections.emptyMap(),
							Map.of("foo", "foo-binder")),
					arguments("onlyBindingProps",
							true,
							Collections.emptyMap(),
							Collections.emptyMap(),
							Map.of("foo", "foo-binding"),
							Map.of("foo", "foo-binding")),
					arguments("binderOverridesBaseValue",
							true,
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-binder"),
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-binder")),
					arguments("binderContainsNewPropWithoutDefaults",
							false,
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base", "bar", "bar-binder"),
							Map.of("foo", "foo-base"),
							Map.of("bar", "bar-binder")),
					arguments("binderContainsNewPropWithDefaults",
							true,
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base", "bar", "bar-binder"),
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base", "bar", "bar-binder")),
					arguments("bindingOverridesBaseValue",
							true,
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-binding"),
							Map.of("foo", "foo-binding")),
					arguments("bindingContainsNewPropWithoutDefaults",
							false,
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base", "bar", "bar-binding"),
							Map.of("bar", "bar-binding")),
					arguments("bindingContainsNewPropWithDefaults",
							true,
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-base", "bar", "bar-binding"),
							Map.of("foo", "foo-base", "bar", "bar-binding")),
					arguments("binderOverridesBaseAndBindingOverridesBinder",
							true,
							Map.of("foo", "foo-base"),
							Map.of("foo", "foo-binder"),
							Map.of("foo", "foo-binding"),
							Map.of("foo", "foo-binding")));
		}
		// @formatter:on

	}

	@Nested
	class MergeProducerPropertiesTests {

		private static final Consumer<ProducerConfigProperties> SET_NO_PROPS = (__) -> {
		};

		@Test
		void noPropsModified() {
			var expectedProps = defaultExtProps();
			doMergeProducerPropertiesTest(SET_NO_PROPS, SET_NO_PROPS, expectedProps);
		}

		// @formatter:off
		@Test
		void basePropModifiedAtBinderLevel() {
			var expectedProps = defaultExtPropsWith("accessMode", ProducerAccessMode.Exclusive);
			doMergeProducerPropertiesTest(
					(binderProps) -> binderProps.setAccessMode(ProducerAccessMode.Exclusive),
					SET_NO_PROPS,
					expectedProps);
		}

		@Test
		void basePropModifiedAtBindingLevel() {
			var expectedProps = defaultExtPropsWith("accessMode", ProducerAccessMode.Exclusive);
			doMergeProducerPropertiesTest(
					SET_NO_PROPS,
					(bindingProps) -> bindingProps.setAccessMode(ProducerAccessMode.Exclusive),
					expectedProps);
		}

		@Test
		void basePropModifiedAtBinderAndBindingLevel() {
			var expectedProps = defaultExtPropsWith("accessMode", ProducerAccessMode.Exclusive);
			doMergeProducerPropertiesTest(
					(binderProps) -> binderProps.setAccessMode(ProducerAccessMode.ExclusiveWithFencing),
					(bindingProps) -> bindingProps.setAccessMode(ProducerAccessMode.Exclusive),
					expectedProps);
		}

		@Test
		void basePropModifiedAtBinderAndBindingLevelWithDefaultValue() {
			var expectedProps = defaultExtProps();
			doMergeProducerPropertiesTest(
					(binderProps) -> binderProps.setAccessMode(ProducerAccessMode.Shared),
					(bindingProps) -> bindingProps.setAccessMode(ProducerAccessMode.Shared),
					expectedProps);
		}

		@Test
		void extPropModifiedAtBinderLevel() {
			var expectedProps = defaultExtPropsWith((p) -> p.setMaxPendingMessages(1200));
			doMergeProducerPropertiesTest(
					(binderProps) -> binderProps.setMaxPendingMessages(1200),
					SET_NO_PROPS,
					expectedProps);
		}

		@Test
		void extPropModifiedAtBindingLevel() {
			var expectedProps = defaultExtPropsWith((p) -> p.setMaxPendingMessages(1200));
			doMergeProducerPropertiesTest(
					SET_NO_PROPS,
					(binderProps) -> binderProps.setMaxPendingMessages(1200),
					expectedProps);
		}

		@Test
		void extPropModifiedAtBinderAndBindingLevel() {
			var expectedProps = defaultExtPropsWith((p) -> p.setMaxPendingMessages(1200));
			doMergeProducerPropertiesTest(
					(binderProps) -> binderProps.setMaxPendingMessages(1100),
					(bindingProps) -> bindingProps.setMaxPendingMessages(1200),
					expectedProps);
		}

		@Test
		void extPropModifiedAtBinderAndBindingLevelWithDefaultValue() {
			var expectedProps = defaultExtProps();
			doMergeProducerPropertiesTest(
					(binderProps) -> binderProps.setMaxPendingMessages(1000),
					(bindingProps) -> bindingProps.setMaxPendingMessages(1000),
					expectedProps);
		}

		@Test
		void baseAndExtPropsAreCombined() {
			var expectedProps = defaultExtPropsWith((p) -> p.setMaxPendingMessages(1200));
			expectedProps.put("accessMode", ProducerAccessMode.Exclusive);
			doMergeProducerPropertiesTest(
					(binderProps) -> binderProps.setAccessMode(ProducerAccessMode.Exclusive),
					(bindingProps) -> bindingProps.setMaxPendingMessages(1200),
					expectedProps);
		}
		// @formatter:on

		private Map<String, Object> defaultExtProps() {
			return new ProducerConfigProperties().toExtendedProducerPropertiesMap();
		}

		private Map<String, Object> defaultExtPropsWith(String key, Object value) {
			var defaultExtProps = defaultExtProps();
			defaultExtProps.put(key, value);
			return defaultExtProps;
		}

		private Map<String, Object> defaultExtPropsWith(Consumer<ProducerConfigProperties> extPropsCustomizer) {
			var extProps = new ProducerConfigProperties();
			extPropsCustomizer.accept(extProps);
			return extProps.toExtendedProducerPropertiesMap();
		}

		private void doMergeProducerPropertiesTest(Consumer<ProducerConfigProperties> binderPropsCustomizer,
				Consumer<ProducerConfigProperties> bindingPropsCustomizer,
				Map<String, Object> expectedMergedProperties) {
			var binderProducerProps = new ProducerConfigProperties();
			binderPropsCustomizer.accept(binderProducerProps);
			var bindingProducerProps = new ProducerConfigProperties();
			bindingPropsCustomizer.accept(bindingProducerProps);
			var mergedProps = PulsarBinderUtils.mergeModifiedProducerProperties(binderProducerProps, bindingProducerProps);
			assertThat(mergedProps).containsExactlyInAnyOrderEntriesOf(expectedMergedProperties);
		}

	}

	@Nested
	class MergeConsumerPropertiesTests {

		private static final Consumer<ConsumerConfigProperties> SET_NO_PROPS = (__) -> {
		};

		@Test
		void noPropsModified() {
			var expectedProps = defaultExtProps();
			doMergeConsumerPropertiesTest(SET_NO_PROPS, SET_NO_PROPS, expectedProps);
		}

		// @formatter:off
		@Test
		void basePropModifiedAtBinderLevel() {
			var expectedProps = defaultExtPropsWith("priorityLevel", 1000);
			doMergeConsumerPropertiesTest(
					(binderProps) -> binderProps.setPriorityLevel(1000),
					SET_NO_PROPS,
					expectedProps);
		}

		@Test
		void basePropModifiedAtBindingLevel() {
			var expectedProps = defaultExtPropsWith("priorityLevel", 1000);
			doMergeConsumerPropertiesTest(
					SET_NO_PROPS,
					(bindingProps) -> bindingProps.setPriorityLevel(1000),
					expectedProps);
		}

		@Test
		void basePropModifiedAtBinderAndBindingLevel() {
			var expectedProps = defaultExtPropsWith("priorityLevel", 1000);
			doMergeConsumerPropertiesTest(
					(binderProps) -> binderProps.setPriorityLevel(2000),
					(bindingProps) -> bindingProps.setPriorityLevel(1000),
					expectedProps);
		}

		@Test
		void basePropModifiedAtBinderAndBindingLevelWithDefaultValue() {
			var expectedProps = defaultExtProps();
			doMergeConsumerPropertiesTest(
					(binderProps) -> binderProps.setPriorityLevel(0),
					(bindingProps) -> bindingProps.setPriorityLevel(0),
					expectedProps);
		}

		@Test
		void extPropModifiedAtBinderLevel() {
			var expectedProps = defaultExtPropsWith((p) -> p.setReceiverQueueSize(1200));
			doMergeConsumerPropertiesTest(
					(binderProps) -> binderProps.setReceiverQueueSize(1200),
					SET_NO_PROPS,
					expectedProps);
		}

		@Test
		void extPropModifiedAtBindingLevel() {
			var expectedProps = defaultExtPropsWith((p) -> p.setReceiverQueueSize(1200));
			doMergeConsumerPropertiesTest(
					SET_NO_PROPS,
					(bindingProps) -> bindingProps.setReceiverQueueSize(1200),
					expectedProps);
		}

		@Test
		void extPropModifiedAtBinderAndBindingLevel() {
			var expectedProps = defaultExtPropsWith((p) -> p.setReceiverQueueSize(1200));
			doMergeConsumerPropertiesTest(
					(binderProps) -> binderProps.setReceiverQueueSize(1100),
					(bindingProps) -> bindingProps.setReceiverQueueSize(1200),
					expectedProps);
		}

		@Test
		void extPropModifiedAtBinderAndBindingLevelWithDefaultValue() {
			var expectedProps = defaultExtProps();
			doMergeConsumerPropertiesTest(
					(binderProps) -> binderProps.setReceiverQueueSize(1000),
					(bindingProps) -> bindingProps.setReceiverQueueSize(1000),
					expectedProps);
		}

		@Test
		void baseAndExtPropsAreCombined() {
			var expectedProps = defaultExtPropsWith((p) -> p.setReceiverQueueSize(1200));
			expectedProps.put("priorityLevel", 1000);
			doMergeConsumerPropertiesTest(
					(binderProps) -> binderProps.setPriorityLevel(1000),
					(bindingProps) -> bindingProps.setReceiverQueueSize(1200),
					expectedProps);
		}
		// @formatter:on

		private Map<String, Object> defaultExtProps() {
			return new ConsumerConfigProperties().toExtendedConsumerPropertiesMap();
		}

		private Map<String, Object> defaultExtPropsWith(String key, Object value) {
			var defaultExtProps = defaultExtProps();
			defaultExtProps.put(key, value);
			return defaultExtProps;
		}

		private Map<String, Object> defaultExtPropsWith(Consumer<ConsumerConfigProperties> extPropsCustomizer) {
			var extProps = new ConsumerConfigProperties();
			extPropsCustomizer.accept(extProps);
			return extProps.toExtendedConsumerPropertiesMap();
		}

		private void doMergeConsumerPropertiesTest(
				Consumer<ConsumerConfigProperties> binderPropsCustomizer,
				Consumer<ConsumerConfigProperties> bindingPropsCustomizer,
				Map<String, Object> expectedMergedProperties) {
			var binderConsumerProps = new ConsumerConfigProperties();
			binderPropsCustomizer.accept(binderConsumerProps);
			var bindingConsumerProps = new ConsumerConfigProperties();
			bindingPropsCustomizer.accept(bindingConsumerProps);
			var mergedProps = PulsarBinderUtils.mergeModifiedConsumerProperties(binderConsumerProps, bindingConsumerProps);
			assertThat(mergedProps).containsExactlyInAnyOrderEntriesOf(expectedMergedProperties);
		}

	}

}
