/*
 * Copyright 2017-2022 the original author or authors.
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

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.actuate.context.properties.ConfigurationPropertiesReportEndpoint;
import org.springframework.boot.actuate.context.properties.ConfigurationPropertiesReportEndpoint.ContextConfigurationProperties;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test primarily validates the correctness of BinderProperties where it as well as
 * what it contains maintains the String-key/Object-value semantics. The use of
 * {@link Properties} class does not exactly do that.
 *
 * @author Oleg Zhurakousky
 *
 */
public class BinderPropertiesTests {

	@SuppressWarnings("unchecked")
	@Test
	@Disabled
	void testSerializationWithNonStringValues() {
		StaticApplicationContext context = new StaticApplicationContext();
		DefaultListableBeanFactory bf = (DefaultListableBeanFactory) context
				.getBeanFactory();
		BindingServiceProperties bindingServiceProperties = new BindingServiceProperties();
		bindingServiceProperties.setApplicationContext(context);
		bf.registerSingleton("bindingServiceProperties", bindingServiceProperties);

		BindingServiceProperties bsp = context.getBean(BindingServiceProperties.class);
		bsp.setApplicationContext(context);
		BinderProperties bp = new BinderProperties();
		bsp.setBinders(Collections.singletonMap("testBinder", bp));
		bp.getEnvironment().put("spring.rabbitmq.connection-timeout", 2345);
		bp.getEnvironment().put("foo", Collections.singletonMap("bar", "hello"));

		// using Spring Boot class to ensure that reliance on the same ObjectMapper
		// configuration
		ConfigurationPropertiesReportEndpoint endpoint = new ConfigurationPropertiesReportEndpoint();
		endpoint.setApplicationContext(context);

		ContextConfigurationProperties configurationProperties = endpoint
				.configurationProperties().getContexts().values().iterator().next();

		Map<String, Object> properties = configurationProperties.getBeans()
				.get("bindingServiceProperties").getProperties();
		assertThat(properties.containsKey("error")).isFalse();
		assertThat(properties.containsKey("binders")).isTrue();
		Map<String, Object> testBinder = (Map<String, Object>) ((Map<String, Object>) properties
				.get("binders")).get("testBinder");
		Map<String, Object> environment = (Map<String, Object>) testBinder
				.get("environment");
		assertThat(
				environment.get("spring.rabbitmq.connection-timeout") instanceof Integer)
						.isTrue();
		assertThat(environment.get("foo") instanceof Map).isTrue();
	}

}
