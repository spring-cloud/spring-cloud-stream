/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.utils;

import java.util.HashMap;
import java.util.Map;

import org.mockito.Mockito;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import static org.mockito.Mockito.when;

/**
 * @author Soby Chacko
 */
@Configuration
public class MockExtendedBinderConfiguration {

	@SuppressWarnings("rawtypes")
	@Bean
	public Binder<?, ?, ?> extendedPropertiesBinder() {
		Binder mock = Mockito.mock(Binder.class,
				Mockito.withSettings().defaultAnswer(Mockito.RETURNS_MOCKS)
						.extraInterfaces(ExtendedPropertiesBinder.class));
		ConfigurableEnvironment environment = new StandardEnvironment();
		Map<String, Object> propertiesToAdd = new HashMap<>();
		propertiesToAdd.put("spring.cloud.stream.foo.default.consumer.extendedProperty",
				"someFancyExtension");
		propertiesToAdd.put("spring.cloud.stream.foo.default.producer.extendedProperty",
				"someFancyExtension");
		environment.getPropertySources()
				.addLast(new MapPropertySource("extPropertiesConfig", propertiesToAdd));

		ConfigurableApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.setEnvironment(environment);

		FooExtendedBindingProperties fooExtendedBindingProperties = new FooExtendedBindingProperties();
		fooExtendedBindingProperties.setApplicationContext(applicationContext);

		final FooConsumerProperties fooConsumerProperties = fooExtendedBindingProperties
				.getExtendedConsumerProperties("input");
		final FooProducerProperties fooProducerProperties = fooExtendedBindingProperties
				.getExtendedProducerProperties("output");

		when(((ExtendedPropertiesBinder) mock).getExtendedConsumerProperties("input"))
				.thenReturn(fooConsumerProperties);

		when(((ExtendedPropertiesBinder) mock).getExtendedProducerProperties("output"))
				.thenReturn(fooProducerProperties);

		return mock;
	}

}
