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

package org.springframework.cloud.stream.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesBindException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.validation.annotation.Validated;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

// see https://github.com/spring-cloud/spring-cloud-stream/issues/1573 for more details
/**
 * @author Oleg Zhurakousky
 *
 */
public class BindingHandlerAdviseTests {

	@Test(expected = BeanCreationException.class)
	public void testFailureWithWrongValue() {
		new SpringApplicationBuilder(SampleConfiguration.class).web(WebApplicationType.NONE).run("--props.value=-1",
				"--spring.jmx.enabled=false");
	}

	@Test
	public void testValidatedValueValue() {
		ValidatedProps validatedProps = new SpringApplicationBuilder(SampleConfiguration.class)
				.web(WebApplicationType.NONE).run("--props.value=2", "--spring.jmx.enabled=false")
				.getBean(ValidatedProps.class);
		assertThat(validatedProps.getValue()).isEqualTo(2);
	}

	@Test
	public void nonValidatedConfigProperties() {
		new SpringApplicationBuilder(NonValidatedConfiguration.class).web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		// simply should not fail
	}

	@Test(expected = ConfigurationPropertiesBindException.class)
	public void validatedConfigProperties() {
		new SpringApplicationBuilder(ValidatedConfiguration.class).web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");

		fail();
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class NonValidatedConfiguration {

		@Bean
		@ConfigurationProperties
		public NonValidatedClass nonValidatedClass() {
			return new NonValidatedClass();
		}
	}

	public static class NonValidatedClass {

		@NotNull
		private String id;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class ValidatedConfiguration {

		@Bean
		@ConfigurationProperties
		public ValidatedClass nonValidatedClass() {
			return new ValidatedClass();
		}
	}

	@Validated
	public static class ValidatedClass {

		@NotNull
		private String id;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}

}

@EnableBinding(Sink.class)
@Import(TestChannelBinderConfiguration.class)
@EnableAutoConfiguration
@EnableConfigurationProperties(ValidatedProps.class)
class SampleConfiguration {

}

@ConfigurationProperties("props")
@Validated
class ValidatedProps {

	@Min(0)
	private int value;

	public int getValue() {
		return this.value;
	}

	public void setValue(int value) {
		this.value = value;
	}

}
