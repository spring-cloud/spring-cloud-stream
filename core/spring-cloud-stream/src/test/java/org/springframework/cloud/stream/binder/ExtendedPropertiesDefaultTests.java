/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Collections;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import org.springframework.cloud.stream.utils.FooConsumerProperties;
import org.springframework.cloud.stream.utils.FooProducerProperties;
import org.springframework.cloud.stream.utils.MockExtendedBinderConfiguration;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
public class ExtendedPropertiesDefaultTests {

	@Test
	void testExtendedDefaultProducerProperties() {
		DefaultBinderFactory binderFactory = createMockExtendedBinderFactory();
		Binder<MessageChannel, ?, ?> binder = binderFactory.getBinder(null,
				MessageChannel.class);
		FooProducerProperties fooProducerProperties = (FooProducerProperties) ((ExtendedPropertiesBinder<?, ?, ?>) binder)
				.getExtendedProducerProperties("output");
		// Expectations are set in the mock configuration for the binder factory
		assertThat(fooProducerProperties.getExtendedProperty())
				.isEqualTo("someFancyExtension");
	}

	@Test
	void testExtendedDefaultConsumerProperties() {
		DefaultBinderFactory binderFactory = createMockExtendedBinderFactory();
		Binder<MessageChannel, ?, ?> binder = binderFactory.getBinder(null,
				MessageChannel.class);
		FooConsumerProperties fooConsumerProperties = (FooConsumerProperties) ((ExtendedPropertiesBinder<?, ?, ?>) binder)
				.getExtendedConsumerProperties("input");
		// Expectations are set in the mock configuration for the binder factory
		assertThat(fooConsumerProperties.getExtendedProperty())
				.isEqualTo("someFancyExtension");
	}

	private DefaultBinderFactory createMockExtendedBinderFactory() {
		BinderTypeRegistry binderTypeRegistry = createMockExtendedBinderTypeRegistry();
		return new DefaultBinderFactory(
				Collections.singletonMap("mock",
						new BinderConfiguration("mock", new HashMap<>(), true, true)),
				binderTypeRegistry, null);
	}

	private DefaultBinderTypeRegistry createMockExtendedBinderTypeRegistry() {
		return new DefaultBinderTypeRegistry(
				Collections.singletonMap("mock", new BinderType("mock",
						new Class[] { MockExtendedBinderConfiguration.class })));
	}

}
