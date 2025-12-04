/*
 * Copyright 2018-present the original author or authors.
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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.StreamRetryTemplate;
import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.test.EnableTestBinder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.support.RetryTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @author Kotaro Matsumoto
 */
class RetryTemplateTests {

	@SuppressWarnings("rawtypes")
	@Test
	void singleCustomRetryTemplate() throws Exception {
		ApplicationContext context = new SpringApplicationBuilder(
			SingleCustomRetryTemplateConfiguration.class).web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false");
		AbstractBinder binder = context.getBean(AbstractBinder.class);
		Field f = AbstractBinder.class.getDeclaredField("consumerBindingRetryTemplates");
		f.setAccessible(true);
		@SuppressWarnings("unchecked")
		Map<String, RetryTemplate> consumerBindingRetryTemplates = (Map<String, RetryTemplate>) f
			.get(binder);
		assertThat(consumerBindingRetryTemplates).hasSize(1);
	}

	@SuppressWarnings("rawtypes")
	@Test
	void specificCustomRetryTemplate() throws Exception {
		ApplicationContext context = new SpringApplicationBuilder(
			SpecificCustomRetryTemplateConfiguration.class)
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.processor-in-0.consumer.retry-template-name=retryTemplateTwo");

		RetryTemplate retryTemplateTwo = context.getBean("retryTemplateTwo",
			RetryTemplate.class);
		BindingServiceProperties bindingServiceProperties = context
			.getBean(BindingServiceProperties.class);
		ConsumerProperties consumerProperties = bindingServiceProperties
			.getConsumerProperties("processor-in-0");
		AbstractBinder binder = context.getBean(AbstractBinder.class);

		Method m = AbstractBinder.class.getDeclaredMethod("buildRetryTemplate",
			ConsumerProperties.class);
		m.setAccessible(true);
		RetryTemplate retryTemplate = (RetryTemplate) m.invoke(binder,
			consumerProperties);
		assertThat(retryTemplate).isEqualTo(retryTemplateTwo);
	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class SpecificCustomRetryTemplateConfiguration {

		@StreamRetryTemplate
		public RetryTemplate retryTemplate() {
			return new RetryTemplate();
		}

		@StreamRetryTemplate
		public RetryTemplate retryTemplateTwo() {
			return new RetryTemplate();
		}

		@Bean
		public RetryTemplate otherRetryTemplate() {
			return new RetryTemplate();
		}

		@Bean
		public Function<String, String> processor() {
			return s -> s;
		}
	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class SingleCustomRetryTemplateConfiguration {

		@StreamRetryTemplate
		public RetryTemplate retryTemplate() {
			return new RetryTemplate();
		}

		@Bean
		public RetryTemplate otherRetryTemplate() {
			return new RetryTemplate();
		}

		@Bean
		public Function<String, String> processor() {
			return s -> s;
		}
	}
}
