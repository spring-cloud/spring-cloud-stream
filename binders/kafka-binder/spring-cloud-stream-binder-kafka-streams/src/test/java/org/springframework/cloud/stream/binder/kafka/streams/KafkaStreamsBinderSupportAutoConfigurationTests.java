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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.CleanupConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link KafkaStreamsBinderSupportAutoConfiguration}.
 *
 * @author wadhwaroh-lang
 */
class KafkaStreamsBinderSupportAutoConfigurationTests {

	@Test
	@SuppressWarnings("unchecked")
	void kafkaStreamsFunctionProcessorUsesAllStreamsBuilderFactoryBeanConfigurers() {
		StreamsBuilderFactoryBeanConfigurer first = mock(StreamsBuilderFactoryBeanConfigurer.class);
		StreamsBuilderFactoryBeanConfigurer second = mock(StreamsBuilderFactoryBeanConfigurer.class);
		ObjectProvider<StreamsBuilderFactoryBeanConfigurer> customizerProvider = mock(ObjectProvider.class);
		ObjectProvider<CleanupConfig> cleanupConfigProvider = mock(ObjectProvider.class);
		when(customizerProvider.orderedStream()).thenReturn(Stream.of(first, second));

		KafkaStreamsFunctionProcessor processor = new KafkaStreamsBinderSupportAutoConfiguration()
				.kafkaStreamsFunctionProcessor(
						mock(BindingServiceProperties.class),
						mock(KafkaStreamsExtendedBindingProperties.class),
						mock(KeyValueSerdeResolver.class),
						mock(KafkaStreamsBindingInformationCatalogue.class),
						mock(KafkaStreamsMessageConversionDelegate.class),
						cleanupConfigProvider,
						mock(StreamFunctionProperties.class),
						mock(KafkaStreamsBinderConfigurationProperties.class),
						customizerProvider,
						mock(ConfigurableEnvironment.class)
				);

		assertThat(processor.customizers).containsExactly(first, second);
	}
}
